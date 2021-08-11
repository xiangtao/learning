package client;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;

public class ClientTest {
  final static Configuration configuration = new Configuration();
  public static void main(String[] args)
      throws CliArgsException, FileNotFoundException, ProgramInvocationException {


    CliFrontend frontend = new CliFrontend(
        configuration,
        Collections.singletonList(new DefaultCLI()));

    String[] arguments = {
        "--classpath", "file:///tmp/foo",
        "--classpath", "file:///tmp/bar",
        "-c", "io.learning.flink.WordCount",
        "-j", "/Users/taox/xdf/git/myself/learning/flink-learning/target/flink-learning-1.0-SNAPSHOT-jar-with-dependencies.jar",
        "true", "arg1", "arg2" };

    CommandLine commandLine = CliFrontendParser
        .parse(CliFrontendParser.getRunCommandOptions(), arguments, true);
    ProgramOptions programOptions = ProgramOptions.create(commandLine);

    PackagedProgram prog = buildProgram(programOptions);
    Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(prog, configuration, 666, true);
    String rst = FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(pipeline);
    System.out.println(rst);
  }

  static PackagedProgram buildProgram(final ProgramOptions runOptions)
      throws FileNotFoundException, ProgramInvocationException, CliArgsException {
    runOptions.validate();

    String[] programArgs = runOptions.getProgramArgs();
    String jarFilePath = runOptions.getJarFilePath();
    List<URL> classpaths = runOptions.getClasspaths();

    // Get assembler class
    String entryPointClass = runOptions.getEntryPointClassName();
    File jarFile = jarFilePath != null ? new File(jarFilePath) : null;

    return PackagedProgram.newBuilder()
        .setJarFile(jarFile)
        .setUserClassPaths(classpaths)
        .setEntryPointClassName(entryPointClass)
        .setConfiguration(configuration)
        .setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
        .setArguments(programArgs)
        .build();
  }

}
