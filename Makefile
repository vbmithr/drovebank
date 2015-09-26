EXE=drovebank.native droveshell.native

all:
	ocamlbuild $(EXE)

.PHONY: clean
clean:
	ocamlbuild -clean
