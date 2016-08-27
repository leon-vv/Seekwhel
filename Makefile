CAML_FLAGS = -I ./build -package postgresql -package calendar -linkpkg -thread

build/seekwhel.cmxa: build/seekwhel.cmx
	ocamlfind ocamlopt -o $@ -a $<

build/seekwhel.cmx: src/seekwhel.ml build/seekwhel.cmi
	ocamlfind ocamlopt $(CAML_FLAGS) -c -o $@ $<

build/seekwhel.cmi: src/seekwhel.mli
	mkdir -p build
	ocamlfind ocamlopt $(CAML_FLAGS) -c -o $@ $<

.PHONY: clean
clean:
	rm -r ./build
