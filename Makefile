EXE=drovebank.native droveshell.native drovestress.native

all:
	ocamlbuild $(EXE)

.PHONY: clean
clean:
	ocamlbuild -clean
