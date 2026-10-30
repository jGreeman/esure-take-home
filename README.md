## Set Up

brew install openjdk@17
set java env variables
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"\

reqs
pyspark
pytest
