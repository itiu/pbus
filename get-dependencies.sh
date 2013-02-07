mq_connector__hash=00ca3e8

mq_connector__project_name=itiu-mq-connector

mkdir build
cd build
mkdir src

wget --no-check-certificate https://github.com/itiu/mq-connector/zipball/$mq_connector__hash
unzip $mq_connector__hash
rm $mq_connector__hash

cd ..

cp -v -r build/$mq_connector__project_name-$mq_connector__hash/src/* build/src


