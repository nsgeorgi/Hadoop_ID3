package id3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ID3 {

    public static double[] tp = new double[4], fn = new double[4], fp = new double[4];
    public static int count = 0; //metritis tis kathe map/reduce faseis 
    public static int count_runs = 0; //metritis gia to poses fores exei treksei o algorithmos
    public static boolean flag = true; //elegxos gia ton termatismo tou algorithmou
    public static String samples_[][] = new String[172][7]; // ta deigmata aksiologisis

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text samples_output = new Text();
        private Text path_output = new Text();
        private String[] tmp = null;//voithitikos pinakas pou kanei split to path
        private String[] tmp1 = null;//voithitikos pinakas pou kanei split to arxeio ekpaideusis
        private String samples[][] = new String[1728 - 172][7];
        private String path = null;// to trexon monopati

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//to value einai to path kai ta deigmata mazi
//praktika douleuoume mono me to value , to key den mas voithaei katholou ston algorithmo
            String tmp_2; 
            String[] str = new String[2];
            str[0] = ("");
            str[1] = ("");

            if (count == 0) { //ean einai 0 simainei oti eimaste stin prwti map/reduce fasi
                path = "";  //opote den uparxei path
                tmp = path.split(" ");  //kanw split to path an kai stin sugkekrimeni periptwsi den xreiazetai
                tmp1 = value.toString().split(",");//kanw split to arxeio ekpaideusis
            } else { //ean uparxei monopati apo proigoumenes map/reduce faseis tote 
                str = value.toString().split("_");//kanw split to monopati kai to arxeio ekpaideusis (xwrizontai me _)
                tmp = str[0].split(","); // to trexon monopati (xwris ,)


                tmp1 = str[1].split(",");  // to arxeio ekpaideusis
                path = str[0];//to trexon monopati (me , )
            }



            int n = 0;
            int v = 0;
            int q = 0;
            int p = 0;
            if (path == "") { //ean den uparxei monopati mexri stigmis
                for (int i = 0; i < tmp1.length; i++) {
                    //sauti ti fasi ginetai to ten-fold cross validation
                    //analoga me to poses fores exei treksei o algorithmos , dimiourgeitai kai to
                    //antistoixo deigma ekpaideusis
                    //px an trexei o algorithmos gia prwti fora tha spasoume to arxeio se 10 kommatia
                    //kai to prwto tha to xrhsimopoihsoume gia test aksiologisis kai ta upoloipa gia ekpaideusi
                    if (i >= (count_runs - 1) * (172 * 7) && i < (count_runs) * (172 * 7)) {
                        if (v >= 7) { //172 einai to 1/10 apo to sunoliko arxeio me ta deigmata
                            v = 0; //7 einai o arithmos twn xaraktiristikwn
                            n++; //epeidi o tmp1 einai monodiastatos to kathe deigma katalamvanei 7 theseis giauto kai 172*7
                        }

                        samples_[n][v] = tmp1[i];  //edw apothikeuetai to test aksiologisis 
                        v++;
                    } else {
                        if (p >= 7) {
                            p = 0;
                            q++;
                        }

                        samples[q][p] = tmp1[i];  //edw apothikeuetai to arxeio gia ekpaideusi diladi ta upoloipa 9
                        p++;
                    }

                }
            } else {
                n = 0;
                v = 0;
                for (int i = 0; i < tmp1.length; i++) {
                    if (v >= 7) {
                        v = 0;
                        n++;
                    }

                    samples[n][v] = tmp1[i];//edw apothikeuetai to arxeio gia ekpaideusi diladi ta upoloipa 9
                    v++;

                }
            }

            tmp1 = null;

            String[] Gain_names = new String[6]; //o pinakas me ta onomata twn xarakthristikwn pou tha upologisoume to Gain tous 
            double[] Gain_table = new double[6];//oi times twn Gain apo ta xarakthristika pou tha upologisoume
            int d = 0;
            for (int i = 0; i < 6; i++) {
                Gain_table[i] = 0.0; //arxikopoihsh
                Gain_names[i] = "";
            }

            if ("".equals(path)) { //ean to path einai keno tote tha upoligsw ta Gain olwn twn xarakthristikwn
                for (int i = 0; i < attributes.length - 1; i++) {
                    Gain_table[d] = Calculate_Gain(values, samples, i);
                    Gain_names[d] = attributes[i];
                    d++;
                }

            } else { //ean to path den einai keno tote tha upologisw ta Gain twn xarakthristikwn pou den uparxoun sto path



                for (int k = 0; k < attributes.length - 1; k++) {
                    boolean found = false;
                    for (int l = 0; l < tmp.length; l++) {


                        if (attributes[k].equals(tmp[l])) {
                            found = true;

                            break;
                        }




                    }
                    if (found == false) {


                        Gain_table[d] = Calculate_Gain(values, samples, k);
                        Gain_names[d] = attributes[k];
                        d++;


                    }

                }
            }
            //KOMBOS !!    
            double max = Gain_table[0];  //apo ta gain pou exw  upologisei , vriskw to max gain
            int pos = 0;
            for (int i = 0; i < Gain_table.length - 1; i++) {
                if (max >= Gain_table[i + 1]) {
                } else {
                    max = Gain_table[i + 1];
                    pos = i + 1;
                }
            }

            int pos_name = 0;  // i thesi tou xarakthristikou pou exei to megalutero gain
            for (int i = 0; i < attributes.length - 1; i++) {
                if (Gain_names[pos].equals(attributes[i])) {
                    pos_name = i;


                    break;
                }
            }

            if (max == 0.0000000) {  //ean to max gain einai 0 auto simainei eite oti to path periexei ola ta xarakthristika eite oti ta deigmata mas anhkoun ola sthn idia klash


                double c1, c2, c3, c4;
                c1 = c2 = c3 = c4 = 0; //metrhtes gia tin kathe klassi
                for (int i = 0; i < samples.length; i++) {  //tha vrw tis suxnotites twn kathe klasewn
                    if ("unacc".equals(samples[i][6])) {
                        c1++;
                    }
                    if ("acc".equals(samples[i][6])) {
                        c2++;
                    }
                    if ("good".equals(samples[i][6])) {
                        c3++;
                    }
                    if ("vgood".equals(samples[i][6])) {
                        c4++;
                    }
                }
                double p1 = (c1 / (c1 + c2 + c3 + c4));
                double p2 = (c2 / (c1 + c2 + c3 + c4));
                double p3 = (c3 / (c1 + c2 + c3 + c4));
                double p4 = (c4 / (c1 + c2 + c3 + c4));
                //ektupwnw to monopati se ena arxeio text
                tmp_2 = path + " Evaluation: Unacc= " + p1 * 100 + "% " + "Acc=" + p2 * 100 + "% " + "Good=" + p3 * 100 + "% " + "Verygood=" + p4 * 100 + "%_";
                path_output.set(tmp_2);
                BufferedWriter out = new BufferedWriter(new FileWriter("paths" + count_runs + ".txt", true));

                out.write(tmp_2);
                out.write("\n");
                out.close();

            } else { //ean to max gain den einai 0 auto shmainei oti exw vrei ena kainourgio xarakthristiko gia na sunexisw to trexon monopati


                for (int i = 0; i < values[pos_name].length; i++) {


                    if (values[pos_name][i] == null) {
                        continue;
                    }
                    String s = "";    //tmp_2 einai to neo path
                    tmp_2 = path + "," + attributes[pos_name] + "," + values[pos_name][i] + "_";

                    for (int m = 0; m < samples.length; m++) {

                        if (values[pos_name][i].equals(samples[m][pos_name])) {

                            for (int w = 0; w < 7; w++) {
                                // s einai to neo arxeio ekpaideusis 
                                s = s + samples[m][w] + ",";
                            }
                        }



                    }

                    String t = tmp_2 + s;
                    path_output.set(t);     //to path_output periexei kai to trexon monopati kai to neo arxeio ekpaideusis  
                    samples_output.set(s);//to samples_output periexei to neo arxeio ekpaideusis
                    context.write(path_output, samples_output);


                    tmp_2 = null;


                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            Text a = new Text();

            for (Text val : values) {
                a.set(val);
            }
            if (a.getLength() != 0) {  //ean to arxeio den einai adeio simainei oti den eimaste se fullo

                flag = true;
                String s;
                s = key.toString();
                Text b = new Text();
                b.set(s);


                context.write(b, null); //grafoume ws key to monopati+arxeio ekpaideushs ka vs value tipota

            } //ean eimaste se fullo tote den kanw tipota , to monopati to egrapsa stin mapper se ksexwristo arxeio




        }
    }
    final static String[][] values = new String[7][4];
    final static String[] attributes = new String[7];

    public static void Start_job(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Id3");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static double Calculate_Gain(String[][] values, String[][] samples, int k) {
        double c1, c2, c3, c4;
        double Total_Entropy = 0;
        c1 = c2 = c3 = c4 = 0;

        for (int i = 0; i < samples.length; i++) {
            if ("unacc".equals(samples[i][6])) {

                c1++;
            }
            if ("acc".equals(samples[i][6])) {
                c2++;

            }
            if ("good".equals(samples[i][6])) {
                c3++;

            }
            if ("vgood".equals(samples[i][6])) {
                c4++;
            }
        }

        double p1 = -(c1 / (c1 + c2 + c3 + c4)) * (Math.log(c1 / (c1 + c2 + c3 + c4)) / Math.log(2));
        double p2 = -(c2 / (c1 + c2 + c3 + c4)) * (Math.log(c2 / (c1 + c2 + c3 + c4)) / Math.log(2));
        double p3 = -(c3 / (c1 + c2 + c3 + c4)) * (Math.log(c3 / (c1 + c2 + c3 + c4)) / Math.log(2));
        double p4 = -(c4 / (c1 + c2 + c3 + c4)) * (Math.log(c4 / (c1 + c2 + c3 + c4)) / Math.log(2));




        if (Double.isNaN(p1)) {
            p1 = 0.0;
        }
        if (Double.isNaN(p2)) {
            p2 = 0.0;
        }
        if (Double.isNaN(p3)) {
            p3 = 0.0;
        }
        if (Double.isNaN(p4)) {
            p4 = 0.0;
        }
        Total_Entropy = p1 + p2 + p3 + p4; //h olikh entropia

        double[] Entropy_attr = new double[4]; // oi entropies kai gia tis 4 times tou xarakthristikou poy eksetazoyme
        double c_all = c1 + c2 + c3 + c4;  //metraei posa deigmata exoume sto arxeio
        double[] new_c_all = new double[4]; //metraei posa deigmata exei i kathe timi tou xarakthristikou
        for (int i = 0; i < 4; i++) {
            Entropy_attr[i] = 0;
            new_c_all[i] = 0;
        }


        for (int j = 0; j < values[k].length; j++) {
            if (values[k][j] == null) {

                continue;
            }
            c1 = c2 = c3 = c4 = 0.0;
            p1 = p2 = p3 = p4 = 0.0;
            for (int i = 0; i < samples.length; i++) {
                if ("unacc".equals(samples[i][6]) && (samples[i][k].equals(values[k][j]))) {
                    c1++;
                }
                if ("acc".equals(samples[i][6]) && (samples[i][k].equals(values[k][j]))) {
                    c2++;
                }
                if ("good".equals(samples[i][6]) && (samples[i][k].equals(values[k][j]))) {
                    c3++;
                }
                if ("vgood".equals(samples[i][6]) && (samples[i][k].equals(values[k][j]))) {
                    c4++;
                }
            }


            p1 = -(c1 / (c1 + c2 + c3 + c4)) * (Math.log(c1 / (c1 + c2 + c3 + c4)) / Math.log(2));
            p2 = -(c2 / (c1 + c2 + c3 + c4)) * (Math.log(c2 / (c1 + c2 + c3 + c4)) / Math.log(2));
            p3 = -(c3 / (c1 + c2 + c3 + c4)) * (Math.log(c3 / (c1 + c2 + c3 + c4)) / Math.log(2));
            p4 = -(c4 / (c1 + c2 + c3 + c4)) * (Math.log(c4 / (c1 + c2 + c3 + c4)) / Math.log(2));




            new_c_all[j] = c1 + c2 + c3 + c4;
            if (Double.isNaN(p1)) {
                p1 = 0.0;
            }
            if (Double.isNaN(p2)) {
                p2 = 0.0;
            }
            if (Double.isNaN(p3)) {
                p3 = 0.0;
            }
            if (Double.isNaN(p4)) {
                p4 = 0.0;
            }



            Entropy_attr[j] = p1 + p2 + p3 + p4;
        }


        double Gain = Total_Entropy - ((new_c_all[0] / c_all) * Entropy_attr[0] + (new_c_all[1] / c_all) * Entropy_attr[1] + (new_c_all[2] / c_all) * Entropy_attr[2] + (new_c_all[3] / c_all) * Entropy_attr[3]);


        return Gain;
    }

    public static void Evaluation(String[] classes, String[][] new_results, int k, int b) {
        for (int i = 0; i < samples_.length; i++) {  //gia kathe deigma aksiologisis upologizw ta fp,fn kai tp


            if (samples_[i][6].equals(classes[k])) { //ean i klasi pou eksetazoume twra einai idia me tou deigmatos



                for (int x = 0; x < b - 1; x++) { //kathe monopati apo to arxeio me ta monopatia pou einai apothikeumeno sto new_results

                    for (int y = 0; y < 7; y++) {

                        if (samples_[i][y].equals(new_results[x][y]) || " ".equals(new_results[x][y]) || "unacc".equals(new_results[x][y]) || "acc".equals(new_results[x][y]) || "good".equals(new_results[x][y]) || "vgood".equals(new_results[x][y])) {
                            if (y == 6) {
                                if (new_results[x][y].equals(classes[k])) { //ean exoume vrei se pio monopati antistoixei to deigma pou eksetazoyme
                                    //tote elegxoyme ena exoun tin idia klasi dld an i provlepsi einai swsti      
                                    tp[k]++; //an einai swsti
                                } else { //an den einai swsti
                                    fn[k]++;
                                    for (int z = 0; z < 4; z++) {

                                        if (classes[z].equals(new_results[x][y])) {
                                            fp[z]++;
                                        }
                                    }
                                }
                            } else {
                                continue; // an den exoume diasxisei akoma olo to monopati tote sunexizoume
                            }
                        } else {
                            break;  //ean den einai akolouthoume to swsto monopati tote pame na elenksoume alla monopatia
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException, ClassNotFoundException {

        attributes[0] = "Buying";
        attributes[1] = "Maintenance";
        attributes[2] = "Doors";
        attributes[3] = "Persons";
        attributes[4] = "Lugboot";
        attributes[5] = "Safety";
        attributes[6] = "Aksiologisi";
        values[0][0] = "vhigh";
        values[0][1] = "high";
        values[0][2] = "med";
        values[0][3] = "low";

        values[1][0] = "vhigh";
        values[1][1] = "high";
        values[1][2] = "med";
        values[1][3] = "low";

        values[2][0] = "2";
        values[2][1] = "3";
        values[2][2] = "4";
        values[2][3] = "5more";

        values[3][0] = "2";
        values[3][1] = "4";
        values[3][2] = "more";

        values[4][0] = "small";
        values[4][1] = "med";
        values[4][2] = "big";

        values[5][0] = "low";
        values[5][1] = "med";
        values[5][2] = "high";

        values[6][0] = "unacc";
        values[6][1] = "acc";
        values[6][2] = "good";
        values[6][3] = "vgood";

        String input;  //to input kai to output gia tis diadoxikes faseis tou map/reduce
        String output;
        double final_precision = 0, final_recall = 0, final_fmeasure = 0; //i teliki timi tis kathe metrikhs
        double[] average_recall = new double[10], average_precision = new double[10], average_fmeasure = new double[10];
        //o mesos oros gia tin kathe metriki


// oi 10 ekteleseis tou algorithmou
        for (int i = 1; i <= 10; i++) {
            count_runs = i;  // metraei tis ekteleseis tou algorithmou
            int x = 0;
            flag = true; //to flag otan ginei true simainei oti oloi oi reducers den dimiourgoun neo arxeio se mia map/reduce fasi opote kai termatizetai o algorithmos
            count = 0; // gia kathe ektelesi tou algorithmou metraei poses map/reduce faseis ginontai
            while (flag == true || count == 0) {
                flag = false;
                if (x == 0) {
                    input = "input/" + "car.data";
                } else {
                    input = "output" + count_runs + "/" + count;
                }
                x = count + 1;
                output = "output" + count_runs + "/" + x;
                Start_job(input, output);
                count++;

            }
            String[][] results = new String[4000][100];
            String[][] new_results = new String[4000][7];
            //diavazw to arxeio me ta monopatia pou dimiourgithike apo ton algorithmo
            Scanner in = new Scanner(new FileReader("paths" + count_runs + ".txt"));
            for (int c = 0; c < results.length; c++) {
                for (int j = 0; j < results[c].length; j++) {
                    results[c][j] = " "; //arxikopoihsh tou pinaka me ta paths apo to arxeio

                }
            }
            for (int c = 0; c < new_results.length; c++) {
                for (int j = 0; j < new_results[c].length; j++) {
                    new_results[c][j] = " "; //kaluteri morfi tou pinaka results ,gia epeksergasia
                }
            }


            int b = 0;
            while (in.hasNextLine()) { //grafw ta monopatia apo to arxeio sto results pinaka kanontas kai split
                String[] str = in.nextLine().split("[:=_ ,%]+");
                results[b] = str;
                b++;
            }

            in.close();

            //Evaluation implementation

            for (int m = 0; m < b - 1; m++) {
                double[] prob = new double[4];

                //antigrafw ton pinaka results ston pinaka new_results  se kaluteri morfh gia epeksergasia

                for (int j = 0; j < results[m].length; j++) {

                    if ("Safety".equals(results[m][j])) {
                        new_results[m][5] = results[m][j + 1];
                    }
                    if ("Lugboot".equals(results[m][j])) {
                        new_results[m][4] = results[m][j + 1];
                    }
                    if ("Maintenance".equals(results[m][j])) {
                        new_results[m][1] = results[m][j + 1];
                    }
                    if ("Persons".equals(results[m][j])) {
                        new_results[m][3] = results[m][j + 1];
                    }
                    if ("Doors".equals(results[m][j])) {
                        new_results[m][2] = results[m][j + 1];
                    }
                    if ("Buying".equals(results[m][j])) {
                        new_results[m][0] = results[m][j + 1];
                    }
                    if ("Unacc".equals(results[m][j])) {
                        prob[0] = Double.parseDouble(results[m][j + 1]);
                    }
                    if ("Acc".equals(results[m][j])) {
                        prob[1] = Double.parseDouble(results[m][j + 1]);
                    }
                    if ("Good".equals(results[m][j])) {
                        prob[2] = Double.parseDouble(results[m][j + 1]);
                    }
                    if ("Verygood".equals(results[m][j])) {
                        prob[3] = Double.parseDouble(results[m][j + 1]);
                        break;
                    }
                }
                double max = prob[0];
                int pos = 0;

                for (int r = 0; r < prob.length - 1; r++) { //gia kathe monopati vriksw tin klassi stin opoia anhkei
                    if (max <= prob[r + 1]) {     // px unacc 0%  acc 0% good 100% vgood %100 
                                                    //ara anhkei sthn good klassi

                        max = prob[r + 1];
                        pos = r + 1;
                    }

                }
               
                new_results[m][6] = "" + values[6][pos];
            }

            String[] classes = new String[4];
            classes[0] = "unacc";
            classes[1] = "acc";
            classes[2] = "good";
            classes[3] = "vgood";

            double[] precision = new double[4], recall = new double[4], fmeasure = new double[4];
            for (int c = 0; c < 4; c++) {
                precision[c] = 0.0;  //arxikopoihsh
                recall[c] = 0.0;
                fmeasure[c] = 0.0;
                tp[c] = 0.0; //true positive
                fp[c] = 0.0; //false positive
                fn[c] = 0.0; //false negative
            }

            for (int c = 0; c < classes.length; c++) {

                Evaluation(classes, new_results, c, b); //upologismos twn tp ,fp kai fn gia kathe klassi


            } //upologizw tis metrikes sumfwna me tous tupous 
            for (int c = 0; c < classes.length; c++) {

                if ((tp[c] + fn[c]) == 0.0) {
                    recall[c] = 0.0;
                } else {
                    recall[c] = tp[c] / (tp[c] + fn[c]);
                }

                if ((tp[c] + fp[c]) == 0.0) {
                    precision[c] = 0.0;
                } else {
                    precision[c] = tp[c] / (tp[c] + fp[c]);
                }

                if ((recall[c] + precision[c] == 0.0)) {
                    fmeasure[c] = 0.0;
                } else {
                    fmeasure[c] = (2 * recall[c] * precision[c]) / (recall[c] + precision[c]);
                }

            } //elegxos ean oi metrikes einai 0

            //mesos oros twn kathe metrikwn
            average_precision[count_runs - 1] = (precision[0] + precision[1] + precision[2] + precision[3]) / 4;
            average_recall[count_runs - 1] = (recall[0] + recall[1] + recall[2] + recall[3]) / 4;
            average_fmeasure[count_runs - 1] = (fmeasure[0] + fmeasure[1] + fmeasure[2] + fmeasure[3]) / 4;

        }
        // oi final metrikes 
        for (int i = 0; i < 10; i++) {
            final_recall = final_recall + average_recall[i];
            final_precision = final_precision + average_precision[i];
            final_fmeasure = final_fmeasure + average_fmeasure[i];
        }
        //o mesos oros twn final metrikwn
        final_recall = (final_recall / 4);
        final_precision = (final_precision / 4);
        final_fmeasure = (final_fmeasure / 4);
        //eggrafi twn metrikwn ston arxeio
        BufferedWriter out = new BufferedWriter(new FileWriter("results.txt"));
        out.write("Final Recall " + final_recall + "  Final Precision " + final_precision + " Final fmeasure " + final_fmeasure);
        out.close();
    }
}
