git pull -p
\rm -rf target/*
mvn package -Dmaven.test.skip=true
#sudo cp target/ticket_synchro.jar /home/q/libs
cp target/ticket_synchro.jar /tmp/
