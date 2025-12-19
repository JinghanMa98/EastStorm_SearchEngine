package cis5550.tools;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Vector;

import cis5550.flame.FlameContext;

/**
 * Utility class to load job classes from JAR files and invoke their run method.
 */
public class Loader {
  
  /**
   * Loads a class from a JAR file and invokes its static run(FlameContext, String[]) method.
   * 
   * @param jarFile The JAR file containing the job class
   * @param className The fully qualified class name (e.g., "cis5550.jobs.Crawler")
   * @param ctx The FlameContext to pass to the job's run method
   * @param args The arguments to pass to the job's run method
   * @throws Exception If the class cannot be loaded or the method cannot be invoked
   */
  public static void invokeRunMethod(File jarFile, String className, FlameContext ctx, Vector<String> args) 
      throws Exception {
    
    // Convert Vector<String> to String[]
    String[] argArray = new String[args.size()];
    args.copyInto(argArray);
    
    // Create a URLClassLoader with the JAR file
    URL jarUrl = jarFile.toURI().toURL();
    URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl}, 
        Thread.currentThread().getContextClassLoader());
    
    try {
      // Load the class
      Class<?> jobClass = classLoader.loadClass(className);
      
      // Get the static run method: public static void run(FlameContext, String[])
      Method runMethod = jobClass.getMethod("run", FlameContext.class, String[].class);
      
      // Invoke the static method
      runMethod.invoke(null, ctx, argArray);
      
    } finally {
      // Close the class loader to release resources
      classLoader.close();
    }
  }
}

