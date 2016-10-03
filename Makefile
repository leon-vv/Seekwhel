CAML_FLAGS = -g -I ./build -I . \
    -package postgresql -package calendar \
    -linkpkg -thread
CAML_MODULE_FILES = \
	seekwhelKeywords \
	seekwhelInner \
	seekwhelColumn \
	seekwhelSelect \
	seekwhelInsert \
	seekwhelUpdate \
	seekwhelDelete

CAML_MODULES = $(addprefix build/, $(addsuffix .cmo, $(CAML_MODULE_FILES)))
CAML_INTERFACES = $(addprefix build/, $(addsuffix .cmi, $(CAML_MODULE_FILES)))

build/seekwhel.cma: build/seekwhelConnection.cmi $(CAML_MODULES) build/seekwhel.cmo
	ocamlfind ocamlc -g -o $@ -a $(CAML_MODULES)

build/seekwhel.cmo: $(CAML_INTERFACES) $(CAML_MODULES) src/seekwhel.ml build/seekwhel.cmi
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ build/keywords.cmo src/seekwhel.ml

build/seekwhelSelect.cmo: src/seekwhelSelect.ml build/seekwhelConnection.cmi build/seekwhelColumn.cmo build/seekwhelSelect.cmi 
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhelInner.cmo: src/seekwhelInner.ml
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhelInsert.cmo: src/seekwhelInsert.ml build/seekwhelSelect.cmo build/seekwhelInsert.cmi
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhelInsert.cmi: src/seekwhelInsert.mli build/seekwhelSelect.cmi 
	mkdir -p build
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhelKeywords.cmo: src/seekwhelKeywords.ml build/seekwhelKeywords.cmi 
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/%.cmo: src/%.ml build/%.cmi
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/seekwhelSelect.cmi: src/seekwhelSelect.mli build/seekwhelColumn.cmi build/seekwhelConnection.cmi 
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/%.cmi: src/%.mli
	mkdir -p build
	ocamlfind ocamlc $(CAML_FLAGS) -c -o $@ $<

build/tests: build/seekwhel.cma tests.ml
	mkdir -p build
	ocamlfind ocamlc -o $@ $(CAML_FLAGS) $^

.PHONY: clean run-tests utop
clean:
	rm -r ./build

utop:
	utop -init ./utop-seekwhel.ml


	


