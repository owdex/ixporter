VENV_DIR := .venv

all: build

$(VENV_DIR)/pyvenv.cfg: ixporter/requirements.txt
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -r ixporter/requirements.txt

venv: $(VENV_DIR)/pyvenv.cfg

dist/ixporter: venv
	$(VENV_DIR)/bin/pip install pyinstaller
	$(VENV_DIR)/bin/pyinstaller --onefile --name ixporter ixporter/__main__.py

build: dist/ixporter

clean: 
	rm -rf $(VENV_DIR) build dist ixporter.spec

/usr/local/bin/ixporter: dist/ixporter
	sudo mv dist/ixporter /usr/local/bin/ixporter

install: /usr/local/bin/ixporter

.PHONY: all build venv install
