GO   ?= go
PKG  ?= ./cmd/verifier
BIN  ?= verifier
FLAGS ?=
ARGS  ?=

.PHONY: build run clean

build:
	$(GO) build -o $(BIN) $(PKG)

run:
	$(GO) run $(PKG) $(FLAGS) $(ARGS)

clean:
	rm -f $(BIN)
