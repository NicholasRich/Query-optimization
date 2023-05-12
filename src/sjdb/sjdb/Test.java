package sjdb;

import java.util.ArrayList;

public class Test {
	private sjdb.Catalogue catalogue;
	
	public Test() {
	}

	public static void main(String[] args) throws Exception {
		sjdb.Catalogue catalogue = createCatalogue();
		sjdb.Inspector inspector = new sjdb.Inspector();
		sjdb.Estimator estimator = new sjdb.Estimator();

		sjdb.Operator plan = query(catalogue);
		plan.accept(estimator);
		plan.accept(inspector);

		sjdb.Optimiser optimiser = new sjdb.Optimiser(catalogue);
		sjdb.Operator planopt = optimiser.optimise(plan);
		planopt.accept(estimator);
		planopt.accept(inspector);
	}
	
	public static sjdb.Catalogue createCatalogue() {
		sjdb.Catalogue cat = new sjdb.Catalogue();
		cat.createRelation("A", 100);
		cat.createAttribute("A", "a1", 100);
		cat.createAttribute("A", "a2", 15);
		cat.createRelation("B", 150);
		cat.createAttribute("B", "b1", 150);
		cat.createAttribute("B", "b2", 100);
		cat.createAttribute("B", "b3", 5);
		
		return cat;
	}

	public static sjdb.Operator query(sjdb.Catalogue cat) throws Exception {
		sjdb.Scan a = new sjdb.Scan(cat.getRelation("A"));
		sjdb.Scan b = new sjdb.Scan(cat.getRelation("B"));
		
		sjdb.Product p1 = new sjdb.Product(a, b);
		
		sjdb.Select s1 = new sjdb.Select(p1, new sjdb.Predicate(new sjdb.Attribute("a2"), new sjdb.Attribute("b3")));

		ArrayList<sjdb.Attribute> atts = new ArrayList<sjdb.Attribute>();
		atts.add(new sjdb.Attribute("a2"));
		atts.add(new sjdb.Attribute("b1"));

		sjdb.Project plan = new sjdb.Project(s1, atts);
		
		return plan;
	}
	
}

