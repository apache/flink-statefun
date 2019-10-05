Stateful Functions Documentation
-------------------------

This documentation is using http://www.sphinx-doc.org/ with the https://pypi.org/project/sphinxcontrib-versioning/ plugin.

Consult the [requirements.txt](requirements.txt) file for the relevant versions of the packages we're using.

# Build the documentation locally

Since we're using the [sphinxcontrib-versioning](https://pypi.org/project/sphinxcontrib-versioning/) plugin to build multiple versions of the documentation, building the docs locally will depend on what you want to achieve.

If you are only concerend with seeing a particular branch/commit that you're working on, then using `make docker-autobuild` will achieve what you want. You can then navigate to http://localhost:8000/index.html to see the resulting pages, and it will auto rebuild the docs on every edit as well.

If the build fails or you experience any issues, try doing a `make clean` to remove any old build artifacts first.

# Export the documentation locally as PDF

`make docker-latexpdf` will export the current brach/commit of the documentation asd PDF. The final documentation will be located under `_build/pdf/latex/ApplicationManager.pdf`.
