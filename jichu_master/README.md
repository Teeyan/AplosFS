# JichuFS

To configure, please change properties in the `fsAppConfig` file.

 sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
 sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
 
 sudo yum install -y apache-maven
 
 // Go to the project directory
 mvn clean install
 
 // Go to target directory
To run use `java -cp jichuFS.jar jichufs.App`
