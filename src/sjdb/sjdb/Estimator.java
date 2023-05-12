package sjdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Estimator implements PlanVisitor {


    public Estimator() {
        // empty constructor
    }

    /*
     * Create output relation on Scan operator
     *
     * Example implementation of visit method for Scan operators.
     */
    public void visit(Scan op) {
        Relation input = op.getRelation();
        Relation output = new Relation(input.getTupleCount());

        Iterator<Attribute> iter = input.getAttributes().iterator();
        while (iter.hasNext()) {
            output.addAttribute(new Attribute(iter.next()));
        }

        op.setOutput(output);
    }

    public void visit(Project op) {
        Relation prevOutput = op.getInput().getOutput();
        Relation output = new Relation(prevOutput.getTupleCount());
        op.getAttributes().forEach(obj -> output.addAttribute(prevOutput.getAttribute(obj)));
        op.setOutput(output);
    }

    public void visit(Select op) {
        Relation prevOutput = op.getInput().getOutput();
        Predicate predicate = op.getPredicate();
        Relation output;
        List<String> attrNames = new ArrayList<>();
        attrNames.add(predicate.getLeftAttribute().getName());
        int valueCount;
        int valueCount1 = prevOutput.getAttribute(predicate.getLeftAttribute()).getValueCount();
        if (predicate.equalsValue()) {
            valueCount = 1;
            output = new Relation(prevOutput.getTupleCount() / valueCount1);
        } else {
            int valueCount2 = prevOutput.getAttribute(predicate.getRightAttribute()).getValueCount();
            output = new Relation(prevOutput.getTupleCount() / Math.max(valueCount1, valueCount2));
            valueCount = Math.min(valueCount1, valueCount2);
            attrNames.add(predicate.getRightAttribute().getName());
        }
        addAttrByNames(output, attrNames, valueCount, prevOutput.getAttributes());
        op.setOutput(output);
    }

    public void visit(Product op) {
        Relation prevOutput1 = op.getLeft().getOutput();
        Relation prevOutput2 = op.getRight().getOutput();
        int tupleCount1 = prevOutput1.getTupleCount();
        int tupleCount2 = prevOutput2.getTupleCount();
        Relation output = new Relation(tupleCount1 * tupleCount2);
        prevOutput1.getAttributes().forEach(output::addAttribute);
        prevOutput2.getAttributes().forEach(output::addAttribute);
        op.setOutput(output);
    }

    public void visit(Join op) {
        Relation prevOutput1 = op.getLeft().getOutput();
        Relation prevOutput2 = op.getRight().getOutput();
        List<Attribute> attributes = new ArrayList<>(prevOutput1.getAttributes());
        attributes.addAll(prevOutput2.getAttributes());
        int tupleCount1 = prevOutput1.getTupleCount();
        int tupleCount2 = prevOutput2.getTupleCount();
        Predicate predicate = op.getPredicate();
        List<String> names = new ArrayList<>();
        names.add(predicate.getLeftAttribute().getName());
        names.add(predicate.getRightAttribute().getName());
        int valueCount1 = attributes.get(attributes.indexOf(predicate.getLeftAttribute())).getValueCount();
        int valueCount2 = attributes.get(attributes.indexOf(predicate.getRightAttribute())).getValueCount();
        int valueCount = Math.min(valueCount1, valueCount2);
        Relation output = new Relation(tupleCount1 * tupleCount2 / Math.max(valueCount1, valueCount2));
        addAttrByNames(output, names, valueCount, attributes);
        op.setOutput(output);
    }

    private void addAttrByNames(Relation relation, List<String> names, int valueCount, List<Attribute> list) {
        for (Attribute attribute : list) {
            for (String name : names) {
                if (attribute.getName().equals(name)) {
                    attribute = new Attribute(name, valueCount);
                }
            }
            relation.addAttribute(attribute);
        }
    }
}
