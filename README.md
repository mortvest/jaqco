# jaqco
(Just Antother Query COmpiler) - Query compiler for ReactDB
## Requirements
* scala
* sbt

Scala uses a sophisticated project building system called sbt. It will update
itself and Scala to the version, necessary for the project and download all the needed packages
when the script "jaqco" from the root directory is ran for the first time.
## Usage
./jaqco -c <file1.jsql, file2.jsql ... > -d <file.ddl> -o <output_directory>
## Show the help message:
./jaqco --help
