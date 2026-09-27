javascript-context.patch applies to e2b-dev/code-interpreter at CI_RECIPE_SHA
and is re-applied on every recipe bump. template/server/main.py creates the
default javascript context only when Jupyter lists a javascript kernelspec;
this flavor ships the python kernel alone, and upstream's server otherwise
fails its startup on the missing kernel.
