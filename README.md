# poliops-scripts-linux

Every night, we place fiscal year data on trfr.aflcio.org for retrieval by Poliops.

Four times a day, we retrieve files specifying approved checks, which Poliops has placed on trfr.aflcio.org. (Some of the files placed may have only a header line, in which case we ignore them.)

This repository has the Python scripts used, and the .ini files that specify the details of pathnames and of connection to trfr.aflcio.org.
