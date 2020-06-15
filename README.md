# Vertica Extractor

A tool to read data from Vertica tables 
and write out [Vertica native binary files](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm). 

This is a companion tool to [Vertica Reader](https://github.com/joeygibson/verticareader), 
which can dump the contents of a native binary file to CSV.
 
**Note:** This is still very early in development, and the code is quite ugly. I'm working 
on making it better. And yes, tests are coming. 

## Usage

```bash
Usage: verticaextractor [options]

Options:
    -s, --server NAME   server to connect to [default: localhost]
    -p, --port NUMBER   port to connect to [default: 5433]
    -d, --database NAME database to extract from
    -u, --username NAME username for login [default: dbadmin]
    -P, --password PASSWORD
                        password for user [default: none]
    -o, --output NAME   output file name
    -f, --force         overwrite destination file
    -t, --table NAME    table to extract
    -l, --limit NUMBER  maximum number of rows to extract from <table>
    -h, --help          display this help message
```

