//=====================================================================
/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/
package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import java.util.Random;

public class MonApplication {
	public static class CopyFile extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			if (args.length != 2) {
				System.err.println("Pas le bon nombre d'argument. Il doit y avoir <source> <destination>.");
				System.exit(-1);
			}
			
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(conf);

			String source = Paths.get(args[0]).toAbsolutePath().normalize().toString();
			
			System.out.println("Copying " + args[0] + " to " + args[1]);

			IOUtils.copyBytes(
				new BufferedInputStream(new FileInputStream(source)),
				fs.create(new Path(args[1])),
				conf,
				true
			);

			return 0;
		}
	}

	public static class ConcatFile extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			if (args.length < 2) {
				System.err.println("Pas le bon nombre d'argument. Il doit y avoir <...sources> <destination>.");
				System.exit(-1);
			}

			Configuration conf = getConf();
			String destinationFile = args[args.length - 1];
			OutputStream destination = FileSystem.get(conf).create(new Path(destinationFile));
			
			for (int index = 0; index < args.length - 1; index++) {
				System.out.println("Copying " + args[index] + " to " + destinationFile);
				String source = Paths.get(args[index]).toAbsolutePath().normalize().toString();

				IOUtils.copyBytes(
					new BufferedInputStream(new FileInputStream(source)),
					destination,
					conf,
					false
				);
			}

			return 0;
		}
	}

	public static class RandomWordGenerator extends Configured implements Tool {
		static String[] syllabes = {
			"ba", "be", "bi"
		};

		public int run(String[] args) throws Exception {
			if (args.length != 3) {
				System.err.println("Pas le bon nombre d'argument. Il doit y avoir <taille_mot> <nombre_mot> <destination_file>.");
				System.exit(-1);
			}

			Configuration conf = getConf();
			OutputStream destination = FileSystem.get(conf).create(new Path(args[2]));
			
			int wordSize = Integer.valueOf(args[0]);
			int wordNumber = Integer.valueOf(args[1]);
			int numberOfSyllabes = syllabes.length;
			
			Random r = new Random();
			for (int i = 0; i < wordNumber; i++) {
				StringBuffer buffer = new StringBuffer();

				for (int j = 0; j < wordSize; j++) {
					buffer.append(syllabes[r.nextInt(numberOfSyllabes)]);
				}

				buffer.append("/n");
				destination.write(String.valueOf(buffer).getBytes());
			}

			destination.close();
			return 0;
		}
	}

	public static void main( String[] args ) throws Exception {
		int returnCode = ToolRunner.run(new MonApplication.RandomWordGenerator(), args);
		System.exit(returnCode);
	}
}
//=====================================================================

