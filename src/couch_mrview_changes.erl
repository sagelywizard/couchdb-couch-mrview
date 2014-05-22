% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.
%
-module(couch_mrview_changes).

-export([go/6, handle_view_changes/5]).

-include_lib("couch/include/couch_db.hrl").

-record(vst, {dbname,
              ddoc,
              view,
              view_options,
              since,
              callback,
              acc,
              user_timeout,
              timeout,
              heartbeat,
              timeout_acc=0,
              notifier,
              stream,
              refresh}).

-type changes_stream() :: true | false | once.
-type changes_options() :: [{stream, changes_stream()} |
                            {since, integer()} |
                            {view_options, list()} |
                            {timeout, integer()} |
                            {heartbeat, true | integer()} |
                            {refresh, true | false}].

-export_type([changes_stream/0]).
-export_type([changes_options/0]).


handle_view_changes(ChangesArgs, Req, Db, DDocId, VName) ->
    %% get view options
    {Query, NoIndex} = case Req of
        {json_req, {Props}} ->
            {Q} = couch_util:get_value(<<"query">>, Props, {[]}),
            NoIndex1 = (couch_util:get_value(<<"use_index">>, Q,
                                            <<"yes">>) =:= <<"no">>),
            {Q, NoIndex1};
        _ ->
            NoIndex1 = couch_httpd:qs_value(Req, "use_index", "yes") =:= "no",
            {couch_httpd:qs(Req), NoIndex1}
    end,
    ViewOptions = parse_view_options(Query, []),

    {ok, Infos} = couch_mrview:get_info(Db, DDocId),
    UpdateOptions = proplists:get_value(update_options, Infos, []),
    IsIndexed = lists:member(<<"seq_indexed">>, UpdateOptions),
    IsKeyIndexed = lists:member(<<"keyseq_indexed">>, UpdateOptions),

    case {IsIndexed, NoIndex} of
        {true, false} ->
            handle_view_changes(Db, DDocId, VName, ViewOptions, ChangesArgs, Req);
        {true, true} when ViewOptions /= [] ->
            ?LOG_ERROR("Tried to filter a non sequence indexed view~n",[]),
            throw({bad_request, seqs_not_indexed});
        {false, _} when ViewOptions /= [] ->
            ?LOG_ERROR("Tried to filter a non sequence indexed view~n",[]),
            throw({bad_request, seqs_not_indexed});
        {_, _} ->
            %% old method we are getting changes using the btree instead
            %% which is not efficient, log it
            ?LOG_WARN("Filter without using a seq_indexed view.~n", []),
            couch_changes:handle_changes(ChangesArgs, Req, Db)
    end.

handle_view_changes(#db{name=DbName}=Db0, DDocId, VName, ViewOptions,
                    ChangesArgs, Req) ->
    #changes_args{
        feed = ResponseType,
        since = Since,
        db_open_options = DbOptions} = ChangesArgs,

    Refresh = refresh_option(Req),

    Options0 = [{since, Since},
                {view_options, ViewOptions},
                {refresh, Refresh}],
    Options = case ResponseType of
        "continuous" -> [stream | Options0];
        "eventsource" -> [stream | Options0];
        "longpoll" -> [{stream, once} | Options0];
        _ -> Options0
    end,

    %% reopen the db with the db options given to the changes args
    couch_db:close(Db0),
    DbOptions1 = [{user_ctx, Db0#db.user_ctx} | DbOptions],
    {ok, Db} = couch_db:open(DbName, DbOptions1),


    %% initialise the changes fun
    fun(Callback) ->
        Callback(start, ResponseType),

        Acc0 = {"", 0, Db, Callback, ChangesArgs},
        go(DbName, DDocId, VName, fun view_changes_cb/2, Acc0, Options)
    end.



%% @doc function returning changes in a streaming fashion if needed.
-spec go(binary(), binary(), binary(), function(), term(),
                     changes_options()) -> ok | {error, term()}.
go(DbName, DDocId, View, Fun, Acc, Options) ->
    Since = proplists:get_value(since, Options, 0),
    Stream = proplists:get_value(stream, Options, false),
    ViewOptions = proplists:get_value(view_options, Options, []),
    %Refresh = proplists:get_value(refresh, Options, false),

    State0 = #vst{
        dbname=DbName,
        ddoc=DDocId,
        view=View,
        view_options=ViewOptions,
        since=Since,
        callback=Fun,
        acc=Acc
    },

    try
        case view_changes_since(State0) of
            {ok, #vst{since=LastSeq, acc=Acc2}=State} ->
                case Stream of
                    true ->
                        start_loop(State#vst{stream=true}, Options);
                    once when LastSeq =:= Since ->
                        start_loop(State#vst{stream=once}, Options);
                    _ ->
                        Fun(stop, {LastSeq, Acc2})
                end;
            {stop, #vst{since=LastSeq, acc=Acc2}} ->
                Fun(stop, {LastSeq, Acc2});
            Error ->
                Error
        end
    after
        ok
    end.

start_loop(#vst{dbname=DbName, ddoc=DDocId}=State, Options) ->
    {UserTimeout, Timeout, Heartbeat} = changes_timeout(Options),
    Notifier = index_update_notifier(DbName, DDocId),
    try
        loop(State#vst{notifier=Notifier,
                       user_timeout=UserTimeout,
                       timeout=Timeout,
                       heartbeat=Heartbeat})
    after
        couch_index_event:stop(Notifier)
    end.

loop(State) ->
    #vst{
        since=Since,
        callback=Callback,
        acc=Acc,
        user_timeout=UserTimeout,
        timeout=Timeout,
        heartbeat=Heartbeat,
        timeout_acc=TimeoutAcc,
        stream=Stream
    } = State,
    receive
        index_update ->
            case view_changes_since(State) of
                {ok, State2} when Stream =:= true ->
                    loop(State2#vst{timeout_acc=0});
                {ok, #vst{since=LastSeq, acc=Acc2}} ->
                    Callback(stop, {LastSeq, Acc2});
                {stop, #vst{since=LastSeq, acc=Acc2}} ->
                    Callback(stop, {LastSeq, Acc2})
            end;
        index_delete ->
            Callback(stop, {Since, Acc})
    after Timeout ->
        TimeoutAcc2 = TimeoutAcc + Timeout,
        case UserTimeout =< TimeoutAcc2 of
            true ->
                Callback(stop, {Since, Acc});
            false when Heartbeat =:= true ->
                case Callback(heartbeat, Acc) of
                    {ok, Acc2} ->
                        loop(State#vst{acc=Acc2, timeout_acc=TimeoutAcc2});
                    {stop, Acc2} ->
                        Callback(stop, {Since, Acc2})
                end;
            _ ->
                Callback(stop, {Since, Acc})
        end
    end.

changes_timeout(Options) ->
    DefaultTimeout = list_to_integer(
        couch_config:get("httpd", "changes_timeout", "60000")
    ),
    UserTimeout = proplists:get_value(timeout, Options, DefaultTimeout),
    {Timeout, Heartbeat} = case proplists:get_value(heartbeat, Options) of
        undefined -> {UserTimeout, false};
        true ->
            T = erlang:min(DefaultTimeout, UserTimeout),
            {T, true};
        H ->
            T = erlang:min(H, UserTimeout),
            {T, true}
    end,
    {UserTimeout, Timeout, Heartbeat}.

view_changes_since(State) ->
    #vst{
        dbname=DbName,
        ddoc=DDocId,
        view=View,
        view_options=Options,
        since=Since,
        callback=Callback,
        acc=UserAcc
    } = State,
    Wrapper = fun ({{Seq, _Key, _DocId}, _Val}=KV, {Go, Acc2, OldSeq}) ->
        LastSeq = if OldSeq < Seq -> Seq;
            true -> OldSeq
        end,

        {Go, Acc3} = Callback(KV, Acc2),
        {Go, {Go, Acc3, LastSeq}}
    end,

    Acc0 = {ok, UserAcc, Since},
    case couch_mrview:view_changes_since(DbName, DDocId, View, Since,
                                         Wrapper, Options, Acc0) of
        {ok, {Go, UserAcc2, Since2}}->
            {Go, State#vst{since=Since2, acc=UserAcc2}};
        Error ->
            Error
    end.

index_update_notifier(#db{name=DbName}, DDocId) ->
    index_update_notifier(DbName, DDocId);
index_update_notifier(DbName, DDocId) ->
    Self = self(),
    {ok, NotifierPid} = couch_index_event:start_link(fun
                ({index_update, {Name, Id, couch_mrview_index}})
                        when Name =:= DbName, Id =:= DDocId ->
                    Self ! index_update;
                ({index_delete, {Name, Id, couch_mrview_index}})
                        when Name =:= DbName, Id =:= DDocId ->
                    Self ! index_delete;
                (_) ->
                    ok
            end),
    NotifierPid.

view_changes_cb(stop, {LastSeq, {_, _, _, Callback, Args}}) ->
    Callback({stop, LastSeq}, Args#changes_args.feed);

view_changes_cb(heartbeat, {_, _, _, Callback, Args}=Acc) ->
    Callback(timeout, Args#changes_args.feed),
    {ok, Acc};
view_changes_cb({{Seq, _Key, DocId}, Val},
                {Prepend, OldLimit, Db0, Callback, Args}=Acc) ->

    %% is the key removed from the index?
    Removed = case Val of
        {[{<<"_removed">>, true}]} -> true;
        _ -> false
    end,

    #changes_args{
        feed = ResponseType,
        limit = Limit} = Args,

    %% if the doc sequence is > to the one in the db record, reopen the
    %% database since it means we don't have the latest db value.
    Db = case Db0#db.update_seq >= Seq of
        true -> Db0;
        false ->
            {ok, Db1} = couch_db:reopen_db(Db0),
            Db1
    end,

    case couch_db:get_doc_info(Db, DocId) of
        {ok, DocInfo} ->
            %% get change row
            {Deleted, ChangeRow} = view_change_row(Db, DocInfo, Args),

            case Removed of
                true when Deleted /= true ->
                    %% the key has been removed from the view but the
                    %% document hasn't been deleted so ignore it.
                    {ok, Acc};
                _ ->
                    %% emit change row
                    Callback({change, ChangeRow, Prepend}, ResponseType),

                    %% if we achieved the limit, stop here, else continue.
                    NewLimit = OldLimit + 1,
                    if Limit > NewLimit ->
                            {ok, {<<",\n">>, NewLimit, Db, Callback, Args}};
                        true ->
                            {stop, {<<"">>, NewLimit, Db, Callback, Args}}
                    end
            end;
        {error, not_found} ->
            %% doc not found, continue
            {ok, Acc};
        Error ->
            throw(Error)
    end.

view_change_row(Db, DocInfo, Args) ->
    #doc_info{id = Id, high_seq = Seq, revs = Revs} = DocInfo,
    [#rev_info{rev=Rev, deleted=Del} | _] = Revs,

    #changes_args{style=Style,
                  include_docs=InDoc,
%                  doc_options = DocOpts,
                  conflicts=Conflicts}=Args,

    Changes = case Style of
        main_only ->
            [{[{<<"rev">>, couch_doc:rev_to_str(Rev)}]}];
        all_docs ->
            [{[{<<"rev">>, couch_doc:rev_to_str(R)}]}
                || #rev_info{rev=R} <- Revs]
    end,

    {Del, {[{<<"seq">>, Seq}, {<<"id">>, Id}, {<<"changes">>, Changes}] ++
     deleted_item(Del) ++ case InDoc of
            true ->
                Opts = case Conflicts of
                    true -> [deleted, conflicts];
                    false -> [deleted]
                end,
                Doc = couch_index_util:load_doc(Db, DocInfo, Opts),
                case Doc of
                    null ->
                        [{doc, null}];
                    _ ->
%                        [{doc, couch_doc:to_json_obj(Doc, DocOpts)}]
                        [{doc, couch_doc:to_json_obj(Doc, [])}]
                end;
            false ->
                []
    end}}.

parse_view_options([], Acc) ->
    Acc;
parse_view_options([{K, V} | Rest], Acc) ->
    Acc1 = case couch_util:to_binary(K) of
        <<"reduce">> ->
            [{reduce, couch_mrview_http:parse_boolean(V)}];
        <<"key">> ->
            V1 = parse_json(V),
            [{start_key, V1}, {end_key, V1} | Acc];
        <<"keys">> ->
            [{keys, parse_json(V)} | Acc];
        <<"startkey">> ->
            [{start_key, parse_json(V)} | Acc];
        <<"start_key">> ->
            [{start_key, parse_json(V)} | Acc];
        <<"startkey_docid">> ->
            [{start_key_docid, couch_util:to_binary(V)} | Acc];
        <<"start_key_docid">> ->
            [{start_key_docid, couch_util:to_binary(V)} | Acc];
        <<"endkey">> ->
            [{end_key, parse_json(V)} | Acc];
        <<"end_key">> ->
            [{end_key, parse_json(V)} | Acc];
        <<"endkey_docid">> ->
            [{start_key_docid, couch_util:to_binary(V)} | Acc];
        <<"end_key_docid">> ->
            [{start_key_docid, couch_util:to_binary(V)} | Acc];
        <<"limit">> ->
            [{limit, couch_mrview_http:parse_pos_int(V)} | Acc];
        <<"count">> ->
            throw({query_parse_error, <<"QS param `count` is not `limit`">>});
        <<"stale">> when V =:= <<"ok">> orelse V =:= "ok" ->
            [{stale, ok} | Acc];
        <<"stale">> when V =:= <<"update_after">> orelse V =:= "update_after" ->
            [{stale, update_after} | Acc];
        <<"stale">> ->
            throw({query_parse_error, <<"Invalid value for `stale`.">>});
        <<"descending">> ->
            case couch_mrview_http:parse_boolean(V) of
                true ->
                    [{direction, rev} | Acc];
                _ ->
                    [{direction, fwd} | Acc]
            end;
        <<"skip">> ->
            [{skip, couch_mrview_http:parse_pos_int(V)} | Acc];
        <<"group">> ->
            case couch_mrview_http:parse_booolean(V) of
                true ->
                    [{group_level, exact} | Acc];
                _ ->
                    [{group_level, 0} | Acc]
            end;
        <<"group_level">> ->
            [{group_level, couch_mrview_http:parse_pos_int(V)} | Acc];
        <<"inclusive_end">> ->
            [{inclusive_end, couch_mrview_http:parse_boolean(V)}];
        _ ->
            Acc
    end,
    parse_view_options(Rest, Acc1).

refresh_option({json_req, {Props}}) ->
    {Query} = couch_util:get_value(<<"query">>, Props),
    couch_util:get_value(<<"refresh">>, Query, true);
refresh_option(Req) ->
    case couch_httpd:qs_value(Req, "refresh", "true") of
        "false" -> false;
        _ -> true
    end.

parse_json(V) when is_list(V) ->
    ?JSON_DECODE(V);
parse_json(V) ->
    V.

deleted_item(true) -> [{<<"deleted">>, true}];
deleted_item(_) -> [].
>>>>>>> f4a208a... gonna do some crazy git-foo later
