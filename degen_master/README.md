# CS 425 MP2

To configure, please change properties in the `mp2AppConfig` file.

 sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
 sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
 
 sudo yum install -y apache-maven
 
 
 // Go to the project directory
 mvn clean install
 
 // Go to target directory
To run use `java -cp cs425mp2.jar edu.illinois.cs425.App`
