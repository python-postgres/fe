# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILD_DIR = ../sphinx

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILD_DIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: help clean dump html web pickle htmlhelp latex changes linkcheck

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html      to make standalone HTML files"
	@echo "  pickle    to make pickle files (usable by e.g. sphinx-web)"
	@echo "  htmlhelp  to make HTML files and a HTML help project"
	@echo "  latex     to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  changes   to make an overview over all changed/added/deprecated items"
	@echo "  linkcheck to check all external links for integrity"

clean:
	-rm -rf *.txt
	-rm -rf ../sphinx

html: dump
	mkdir -p $(BUILD_DIR) $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILD_DIR)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILD_DIR)."

pickle: dump
	mkdir -p $(BUILD_DIR)/pickle $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b pickle $(ALLSPHINXOPTS) $(BUILD_DIR)/pickle
	@echo
	@echo "Build finished; now you can process the pickle files or run"
	@echo "  sphinx-web $(BUILD_DIR)/pickle"
	@echo "to start the sphinx-web server."

web: pickle

htmlhelp: dump
	mkdir -p $(BUILD_DIR)/htmlhelp $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b htmlhelp $(ALLSPHINXOPTS) $(BUILD_DIR)/htmlhelp
	@echo
	@echo "Build finished; now you can run HTML Help Workshop with the" \
	      ".hhp project file in $(BUILD_DIR)/htmlhelp."

latex: dump
	mkdir -p $(BUILD_DIR)/latex $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) $(BUILD_DIR)/latex
	@echo
	@echo "Build finished; the LaTeX files are in $(BUILD_DIR)/latex."
	@echo "Run \`make all-pdf' or \`make all-ps' in that directory to" \
	      "run these through (pdf)latex."

changes: dump
	mkdir -p $(BUILD_DIR)/changes $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b changes $(ALLSPHINXOPTS) $(BUILD_DIR)/changes
	@echo
	@echo "The overview file is in $(BUILD_DIR)/changes."

linkcheck: dump
	mkdir -p $(BUILD_DIR)/linkcheck $(BUILD_DIR)/doctrees
	$(SPHINXBUILD) -b linkcheck $(ALLSPHINXOPTS) $(BUILD_DIR)/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in $(BUILD_DIR)/linkcheck/output.txt."

mkdirs:
	mkdir -p .static .templates

dump: mkdirs
	cp ../postgresql/documentation/*.txt ./
	cp index.rst index.txt
	#cp modules.rst modules.txt
