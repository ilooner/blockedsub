/**
 * Put your copyright and license info here.
 */
package com.datatorrent.blockedsub;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="Blocked Sub_12")
public class Application1 implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    ConsumerOperator cons = dag.addOperator("console", new ConsumerOperator());
    cons.setPartitionCount(12);

    dag.addStream("randomData", randomGenerator.out, cons.input);
  }
}
