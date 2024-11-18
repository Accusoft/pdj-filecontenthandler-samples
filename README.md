# PrizmDoc for Java Sample File Content Handler 
By default, VirtualViewer will use its built-in sample handler. This example contains that code that you can build your handler from. This repository also contains a file content handler that utilizes S3. Please note that these are examples. 

## Requirements
* Java JDK 1.8 or newer
    * PrizmDoc for Java 5.3 and 5.4 require JDK 11
* [Apache Maven](https://maven.apache.org/)
    * To make development easier, the `mvn` command should be part of your system `PATH` variable.
    * On most Linux distros, this will be added automaticly if you install via the package manager.
    * On Windows, add the Maven `bin` directory to your PATH environmental variable.

## Configure web.xml
In web.xml, change the `contentHandlerClass` value to the name of your new content handler. In this example, we will use `com.snowbound.virtualviewer.contenthandler.example.FileContentHandler`





