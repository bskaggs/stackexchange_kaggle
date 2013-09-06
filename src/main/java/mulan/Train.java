package mulan;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import mulan.classifier.lazy.MLkNN;
import mulan.classifier.meta.RAkEL;
import mulan.classifier.transformation.LabelPowerset;
import mulan.data.LabelNodeImpl;
import mulan.data.LabelsMetaDataImpl;
import mulan.data.MultiLabelInstances;
import mulan.evaluation.Evaluator;
import mulan.evaluation.MultipleEvaluation;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.MultiFilter;
import weka.filters.unsupervised.attribute.NumericToBinary;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class Train {
	public static void main(String[] args) throws Exception {
		Class.forName("org.sqlite.JDBC");
		Connection connection = DriverManager.getConnection(args[0]);
		
		String query = "SELECT title, body, tags FROM questions LIMIT 10000;";
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(query);

		ArrayList<Attribute> attInfo = new ArrayList<Attribute>();
		attInfo.add(new Attribute("title", (ArrayList) null));
		attInfo.add(new Attribute("body", (ArrayList) null));
		attInfo.add(new Attribute("tags", (ArrayList) null));
		Instances data = new Instances("training", attInfo, 0);
		
		int count = 0;
		while (result.next()) {
			double[] newInst = new double[3];
			newInst[0] = (double) data.attribute(0).addStringValue(result.getString("title"));
			newInst[1] = (double) data.attribute(1).addStringValue(result.getString("body"));
			newInst[2] = (double) data.attribute(2).addStringValue(result.getString("tags"));
			data.add(new DenseInstance(1.0, newInst));
	
			if (count % 10000 ==0 ) {
				System.out.print(count);
			} else 	if (count % 1000 == 0 ) {
				System.out.print(".");
			}
			count++;
		}
		
		result.close();
		statement.close();
		connection.close();
		
				
		Filter[] filters = new Filter[4];
		for (int i = 0; i < filters.length - 1; i++) {
			StringToWordVector filter = new StringToWordVector();
			filter.setAttributeIndicesArray(new int[] {i});
			filter.setAttributeNamePrefix(i + "_");
			filter.setInputFormat(data);
			filter.setUseStoplist(true);
			filter.setLowerCaseTokens(true);
			filter.setWordsToKeep(10000);
			filters[(filters.length - 2) - i] = filter;
		}
		{
			NumericToBinary filter = new NumericToBinary();
			filter.setInputFormat(data);
			filters[3] = filter;
		}
		
		
		MultiFilter mf = new MultiFilter();
		mf.setFilters(filters);
		mf.setInputFormat(data);
		Instances data2 = Filter.useFilter(data, mf);
		

		LabelsMetaDataImpl lmd = new LabelsMetaDataImpl();
		for (int j = 0; j < data2.numAttributes(); j++) {
			Attribute attr = data2.attribute(j);
			String name = attr.name();
			if (name.startsWith("2_")) {
				lmd.addRootNode(new LabelNodeImpl(name));
			}
		}
	
		MultiLabelInstances dataset = new MultiLabelInstances(data2, lmd);
		
        RAkEL learner2 = new RAkEL(new LabelPowerset(new J48()));
        MLkNN learner1 = new MLkNN();

        Evaluator eval = new Evaluator();
        MultipleEvaluation results;

        int numFolds = 10;
        results = eval.crossValidate(learner1, dataset, numFolds);
        System.out.println(results);
        results = eval.crossValidate(learner2, dataset, numFolds);
        System.out.println(results);
	}
}