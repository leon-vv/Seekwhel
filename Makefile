OCB_FLAGS = -tag bin_annot -I maintenance -I src -use-ocamlfind -cflag -thread
OCB = ocamlbuild $(OCB_FLAGS)

_build/src/seekwhel.cmxa: src/*
	$(OCB) seekwhel.cmxa

_build/src/seekwhel.cma: src/*
	$(OCB) seekwhel.cma

_build/maintenance/benchmark: maintenance/benchmark.ml src/*
	$(OCB) benchmark.native

.PHONY: test
test:
	sh ./maintenance/run-tests.sh
