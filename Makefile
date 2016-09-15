CAML_FLAGS = -g -I ./build -I . \
    -package postgresql -package calendar \
    -linkpkg -thread

build/seekwhel.cma: build/keywords.cmo build/seekwhel.cmo
	ocamlfind ocamlc -g -o $@ -a $^

build/%.cmi: src/%.mli
	mkdir -p build
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhel.cmo: build/keywords.cmi build/keywords.cmo src/seekwhel.ml build/seekwhel.cmi
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ build/keywords.cmo src/seekwhel.ml

build/%.cmo: src/%.ml build/%.cmi
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/tests: build/seekwhel.cma tests.ml
	mkdir -p build
	ocamlfind ocamlc -o $@ $(CAML_FLAGS) $^

.PHONY: clean run-tests utop
clean:
	rm -r ./build

utop:
	utop -init ./utop-seekwhel.ml


	


