package dataservices;

import java.io.Reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.impl.BaseProxy;

//IMPORTANT: Do not edit. This file is generated.

import com.marklogic.client.io.Format;

/**
* Data services test
*/
public interface Test {
/**
* Creates a Test object for executing operations on the database server.
*
* The DatabaseClientFactory class can create the DatabaseClient parameter. A single
* client object can be used for any number of requests and in multiple threads.
*
* @param db	provides a client for communicating with the database server
* @return	an object for session state
*/
static Test on(DatabaseClient db) {
   final class TestImpl implements Test {
       private BaseProxy baseProxy;

       private TestImpl(DatabaseClient dbClient) {
           baseProxy = new BaseProxy(dbClient, "/data-services/test/");
       }

       @Override
       public Reader hello(String name) {
         return BaseProxy.JsonDocumentType.toReader(
           baseProxy
           .request("hello.sjs", BaseProxy.ParameterValuesKind.SINGLE_ATOMIC)
           .withSession()
           .withParams(BaseProxy.atomicParam("name", true, BaseProxy.StringType.fromString(name)))
           .withMethod("POST")
           .responseSingle(false, Format.JSON)
           );
       }

   }

   return new TestImpl(db);
}

/**
* Hello World
*
* @param name	provides input
* @return	as output
*/
Reader hello(String name);

}


