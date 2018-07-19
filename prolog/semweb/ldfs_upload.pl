:- module(
  ldfs_upload,
  [
    ldfs_upload/1 % +Base:oneof([data,meta])
  ]
).

/** <module> Linked Data File System (LDFS): Upload

@author Wouter Beek
@version 2018
*/

:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(zlib)).

:- use_module(library(file_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(tapir/tapir_api)).

:- curl.
:- debug(ldfs_upload).





%! ldfs_upload(+Base:oneof([data,meta])) is det.

ldfs_upload(Base) :-
  must_be(oneof([data,meta]), Base),
  ldfs_root(Dir0),
  directory_file_path(Dir0, tmp, Dir),
  create_directory(Dir),
  file_name_extension(Base, 'nq.gz', Local),
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
  ),
  (   is_empty_file(TmpFile)
  ->  true
  ;   dataset_upload(_, _, metadata, _{accessLevel: public, files: [TmpFile]})
  ),
  delete_file(TmpFile).
