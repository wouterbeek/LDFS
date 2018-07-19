:- module(
  ldfs_upload,
  [
    ldfs_upload/0,
    ldfs_upload/1  % +Kind
  ]
).
:- reexport(library(semweb/ldfs)).

/** <module> Linked Data File System (LDFS): Upload

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(zlib)).

:- use_module(library(file_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(tapir/tapir_api)).
:- use_module(library(thread_ext)).

:- curl.
:- debug(ldfs_upload).





%! ldfs_upload is det.

ldfs_upload :-
  setup_call_cleanup(
    threaded_maplist(ldfs_upload_file, [data,error,meta,warning], Files1),
    (
      exclude(is_empty_file, Files1, Files2),
      dataset_upload(_, _, metadata, _{accessLevel: public, files: [Files2]})
    ),
    maplist(delete_file, Files1)
  ).


%! ldfs_upload(+Kind:oneof([data,error,meta,warning])) is det.

ldfs_upload(Kind) :-
  must_be(oneof([data,error,meta,warning]), Kind),
  setup_call_cleanup(
    ldfs_upload_file(Kind, File),
    (   is_empty_file(File)
    ->  true
    ;   dataset_upload(_, _, metadata, _{accessLevel: public, files: [File]})
    ),
    delete_file(File)
  ).





% HELPERS %

%! ldfs_upload_file(+Kind:oneof([data,error,meta,warning]), -File:atom) is det.

ldfs_upload_file(Kind, File) :-
  ldfs_root(Dir0),
  directory_file_path(Dir0, tmp, Dir),
  create_directory(Dir),
  file_name_extension(Kind, 'nq.gz', Local),
  directory_file_path(Dir, Local, TmpFile),
  setup_call_cleanup(
    gzopen(TmpFile, write, Out),
    forall(
      ldfs_file(Local, File),
      (
        debug(ldfs, "~a → ~a", [File,TmpFile]),
        setup_call_cleanup(
          gzopen(File, read, In),
          copy_stream_data(In, Out),
          close(In)
        )
      )
    ),
    close(Out)
  ).
