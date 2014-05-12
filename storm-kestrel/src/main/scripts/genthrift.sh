rm -rf gen-javabean
rm -rf ../java/com/ss/kestrel/thrift
thrift --gen java:beans,hashcode,nocamel kestrel.thrift
mv gen-javabean/com/ss/kestrel/thrift ../java/com/ss/kestrel/thrift
rm -rf gen-javabean