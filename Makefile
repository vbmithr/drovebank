EXE=drovebank.native droveshell.native

all:
	ocamlbuild -use-ocamlfind $(EXE)

.PHONY: clean
clean:
	ocamlbuild -clean
