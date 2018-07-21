:- module(
  ldfs_upload,
  [
    ldfs_cleanup/0,
    ldfs_upload_data/0,
    ldfs_upload_dataset/1, % +Dataset
    ldfs_upload_meta/0
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

:- use_module(library(atom_ext)).
:- use_module(library(dcg)).
:- use_module(library(file_ext)).
:- use_module(library(hash_ext)).
:- use_module(library(http/http_client2)).
:- use_module(library(ll/index_api)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(string_ext)).
:- use_module(library(tapir/tapir_api)).
:- use_module(library(thread_ext)).
:- use_module(library(uri_ext)).

:- curl.
:- debug(ldfs).

:- maplist(rdf_register_prefix, [
     data-'https://index.lodlaundromat.org/dataset/',
     dcterm,
     file-'https://index.lodlaundromat.org/file/',
     id-'https://lodlaundromat.org/id/',
     ldm-'https://ldm.cc/',
     ll-'https://lodlaundromat.org/def/',
     rdf
   ]).

:- rdf_meta
   ldfs_upload_dataset(r).





%! ldfs_cleanup is det.

ldfs_cleanup :-
  current_user(User),
  forall(
    (
      organization(_, Org, Dict),
      Dict.members == [User]
    ),
    (
      forall(
        dataset(_, Org, Dataset, _),
        dataset_delete(_, Org, Dataset)
      ),
      organization_delete(_, User, Org)
    )
  ).



%! ldfs_upload_data is det.

ldfs_upload_data :-
  forall(
    index_dataset(Dataset),
    ldfs_upload_dataset(Dataset)
  ).

ldfs_upload_dataset(Dataset) :-
  create_dataset_organization(Dataset, OrgName),
  % Compose the dataset description.
  index_statement(Dataset, rdfs:label, DatasetName1),
  rdf_literal_lexical_form(DatasetName1, DatasetName2),
  triply_name(DatasetName2, DatasetName3),
  % Not every dataset has a description yet.
  (   index_statement(Dataset, dcterm:description, Desc0)
  ->  rdf_literal_lexical_form(Desc0, Desc)
  ;   Desc = ""
  ),
  dataset_url(Dataset, Url),
  % Not every dataset has a homepage yet.
  (   index_statement(Dataset, foaf:homepage, Homepage0)
  ->  rdf_literal_lexical_form(Homepage0, Homepage),
      format(string(HomepageLabel), "Homepage: [~a](~a)\n\n", [Homepage,Homepage])
  ;   HomepageLabel = ""
  ),
  format(string(Description), "[~a](~a)\n\n~s~a", [DatasetName2,Url,HomepageLabel,Desc]),
  string_ellipsis(Description, 1 000, EllipsedDescription),
  findall(
    File,
    (
      index_statement(Dataset, ldm:distribution, Distribution),
      distribution_file(Distribution, File)
    ),
    Files
  ),
  (   Files == []
  ->  true
  ;   uri_comps(
        Prefix,
        uri(https,'data.lodlaundromat.org',[OrgName,DatasetName3,graphs],_,_)
      ),
      Props = _{
        accessLevel: public,
        description: EllipsedDescription,
        files: Files,
        prefixes: [graph-Prefix]
      },
      dataset_upload(_, OrgName, DatasetName3, Props)
  ).

create_dataset_organization(Dataset, OrgName3) :-
  index_statement(Dataset, dcterm:creator, Org),
  index_statement(Org, rdfs:label, OrgName1),
  rdf_literal_lexical_form(OrgName1, OrgName2),
  triply_name(OrgName2, OrgName3),
  organization_create(_, _, OrgName3, _{name: OrgName2}, _).
  % TBD: Must go through Triply Client now.
  %index_statement(Org, foaf:depiction, literal(type(xsd:anyURI,Url))),
  %account_property(_, OrgName3, avatar(Url), _).

dataset_url(Dataset, Url) :-
  current_user(User),
  uri_comps(
    Url,
    uri(https,'data.lodlaundromat.org',[User,index,browser],[resource(Dataset)],_)
  ).

distribution_file(Distribution, DataFile) :-
  index_statement(Distribution, ldm:file, File),
  index_statement(File, ldm:downloadLocation, literal(type(xsd:anyURI,Url))),
  md5(Url, ArchHash),
  rdf_prefix_iri(id:ArchHash, Arch),
  archive_entry(Arch, Entry),
  rdf_prefix_iri(id:EntryHash, Entry),
  ldfs_file(EntryHash, 'data.nq.gz', DataFile),
  \+ is_empty_file(DataFile).

archive_entry(Arch, Entry) :-
  metadata_statement(Arch, ll:entry, Entry0), !,
  archive_entry(Entry0, Entry).
archive_entry(Entry, Entry).



%! ldfs_upload_meta is det.

ldfs_upload_meta :-
  ldfs_upload([error,meta,warning]).





% HELPERS %

%! ldfs_upload(+Kinds:list(oneof([data,error,meta,warning])) is det.

ldfs_upload(Kinds) :-
  must_be(list(oneof([data,error,meta,warning])), Kinds),
  setup_call_cleanup(
    threaded_maplist(ldfs_upload_file, Kinds, Files1),
    (
      exclude(is_empty_file, Files1, Files2),
      dataset_upload(_, _, metadata, _{accessLevel: public, files: Files2})
    ),
    maplist(delete_file, Files1)
  ).



%! ldfs_upload_file(+Kind:oneof([data,error,meta,warning]), -File:atom) is det.

ldfs_upload_file(Kind, File) :-
  ldfs_root(Dir0),
  directory_file_path(Dir0, tmp, Dir),
  create_directory(Dir),
  file_name_extension(Kind, 'nq.gz', Local),
  directory_file_path(Dir, Local, File),
  setup_call_cleanup(
    gzopen(File, write, Out),
    forall(
      ldfs_file(Local, File0),
      (
        debug(ldfs, "~a → ~a", [File0,File]),
        setup_call_cleanup(
          gzopen(File0, read, In),
          copy_stream_data(In, Out),
          close(In)
        )
      )
    ),
    close(Out)
  ).



%! metadata_statement(?S:rdf_nonliteral, ?P:iri, ?O:rdf_term) is nondet.

metadata_statement(S, P, O) :-
  current_user(User),
  statement(_, User, metadata, S, P, O).



%! triply_name(+Name:atom, -TriplyName:atom) is det.
%
% Triply names can only contain alpha-numeric characters and hyphens.

triply_name(Name1, Name3) :-
  atom_phrase(triply_name_, Name1, Name2),
  atom_truncate(Name2, 40, Name3).

triply_name_, [Code] -->
  [Code],
  {
    code_type(Code, ascii),
    code_type(Code, alnum)
  }, !,
  triply_name_.
triply_name_, "-" -->
  [_], !,
  triply_name_.
triply_name_--> "".
