package sjdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Optimiser {
    private Catalogue catalogue;
    private List<Scan> joinTree;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator plan) {
        List<Select> selects = new ArrayList<>();
        List<Select> joins = new ArrayList<>();
        List<Scan> scans = new ArrayList<>();
        List<Project> projects = new ArrayList<>();
//        1.Parse the plan, get all the Operators and aad them to the relevant list
        parsePlan(plan, projects, scans, joins, selects);
//        Get all the project attributes to determine whether the final result needs to be surrounded by "Project()"
        List<Attribute> projectAttrs = getProjectAttr(projects);
//        2.Sort the scan
        scans = sortScan(scans, selects);
//        Get the first scan which needs to be handled
        Scan first = scans.get(0);
//        Remove the first scan from the list
        scans = cleanScan(scans, first);
//        Create a list to save the ordered scans
        List<Scan> orderedScans = new ArrayList<>();
        orderedScans.add(first);
//        Create a list to save the ordered joins
        List<Select> orderedJoins = new ArrayList<>();
//        3.Sort the scan and join
        sortScanAndJoin(scans, joins, first, orderedScans, orderedJoins, selects);
//        Transform all the ordered scans to operators with applying select and project on each item
        List<Operator> unaryList = getUnaryList(selects, projectAttrs, orderedScans, orderedJoins);
//        Apply join on the ordered operators
        Operator operator = executeJoin(orderedJoins, unaryList);
//        If there is no project attributes or no join, return the result
        if (projectAttrs.size() == 0 || unaryList.size() == 1) {
            return operator;
        }
//        Else it means there is a project, return the result surrounded by "Project()"
        return new Project(operator, projectAttrs);
    }

    private List<Attribute> getProjectAttr(List<Project> projects) {
        List<Attribute> attributes = new ArrayList<>();
        for (Project project : projects) {
            attributes.addAll(project.getAttributes());
        }
        return attributes;
    }

    private Operator executeJoin(List<Select> orderedJoins, List<Operator> unaryList) {
        Operator operator = unaryList.get(0);
        int index = 1;
        for (Select orderedJoin : orderedJoins) {
            Operator left = operator;
            Operator right = unaryList.get(index++);
            operator = new Join(left, right, orderedJoin.getPredicate());
        }
        return operator;
    }

    private List<Operator> getUnaryList(List<Select> selects, List<Attribute> attributes, List<Scan> orderedScans,
                                        List<Select> orderedJoins) {
        List<Operator> unaryList = new ArrayList<>();
        for (Scan scan : orderedScans) {
            Operator select = setSelect(scan, selects);
            Operator operator = setProject(attributes, scan, orderedJoins, select);
            unaryList.add(operator);
        }
        return unaryList;
    }

    private void parsePlan(Operator plan, List<Project> projects, List<Scan> scans, List<Select> joins,
                           List<Select> selects) {
        if (plan instanceof Select) {
            addSelectByRight(joins, selects, plan);
        } else if (plan instanceof Scan) {
            scans.add((Scan) plan);
        } else if (plan instanceof Project) {
            projects.add((Project) plan);
        }
        List<Operator> inputs = plan.getInputs();
        if (inputs != null) {
            for (Operator input : inputs) {
                parsePlan(input, projects, scans, joins, selects);
            }
        }
    }

    private Operator setSelect(Scan scan, List<Select> selects) {
        for (Select select : selects) {
            if (existAttr(scan, select.getPredicate().getLeftAttribute())) {
                return new Select(scan, select.getPredicate());
            }
        }
        return scan;
    }

    private Operator setProject(List<Attribute> attributes, Scan scan, List<Select> joins, Operator operator) {
        if (attributes.size() == 0) {
            return operator;
        }
        List<Attribute> target = new ArrayList<>();
        for (Attribute attribute : attributes) {
            if (existAttr(scan, attribute)) {
                target.add(attribute);
            }
        }
        for (Select join : joins) {
            Attribute left = join.getPredicate().getLeftAttribute();
            Attribute right = join.getPredicate().getRightAttribute();
            if (existAttr(scan, left) && !target.contains(left)) {
                target.add(left);
            }
            if (existAttr(scan, right) && !target.contains(right)) {
                target.add(right);
            }
        }
        return new Project(operator, target);
    }

    private boolean existAttr(Scan scan, Attribute attribute) {
        try {
            scan.getRelation().getAttribute(attribute);
        } catch (IndexOutOfBoundsException e) {
            return false;
        }
        return true;
    }

    private void sortScanAndJoin(List<Scan> scans, List<Select> joins, Scan first,
                                 List<Scan> orderedScans, List<Select> orderedJoins, List<Select> selects) {
        if (scans.size() == 0) {
            return;
        }
        List<Select> rawJoins = getJoinsByScan(joins, first);
        List<Scan> rawScans = getScansByJoin(scans, rawJoins, first);
        rawScans = sortScan(rawScans, selects);
        rawJoins = sortJoin(rawScans, rawJoins);
        scans = cleanScan(scans, rawScans);
        joins = cleanJoin(joins, first);
        orderedJoins.addAll(rawJoins);
        orderedScans.addAll(rawScans);
        first = rawScans.get(0);
        sortScanAndJoin(scans, joins, first, orderedScans, orderedJoins, selects);
    }


    private List<Select> cleanJoin(List<Select> joins, Scan scan) {
        return joins.stream().filter(join -> {
            Attribute left = join.getPredicate().getLeftAttribute();
            Attribute right = join.getPredicate().getRightAttribute();
            return !existAttr(scan, left) && !existAttr(scan, right);
        }).collect(Collectors.toList());
    }

    private List<Scan> cleanScan(List<Scan> scans, List<Scan> rawScans) {
        List<Attribute> attributes = new ArrayList<>();
        for (Scan rawScan : rawScans) {
            attributes.addAll(rawScan.getRelation().getAttributes());
        }
        return scans.stream().filter(scan -> {
            for (Attribute attribute : attributes) {
                if (existAttr(scan, attribute)) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
    }

    private List<Scan> cleanScan(List<Scan> scans, Scan target) {
        List<Attribute> attributes = new ArrayList<>(target.getRelation().getAttributes());
        return scans.stream().filter(scan -> {
            for (Attribute attribute : attributes) {
                if (existAttr(scan, attribute)) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
    }

    private List<Select> sortJoin(List<Scan> scans, List<Select> joins) {
        List<Select> orderedJoins = new ArrayList<>();
        for (Scan scan : scans) {
            orderedJoins.addAll(getJoinsByScan(joins, scan));
        }
        return orderedJoins;
    }

    private List<Scan> sortScan(List<Scan> scans, List<Select> valueSelects) {
        List<Scan> orderedScans = new ArrayList<>();
        scans = scans.stream().filter(scan -> {
            for (Select valueSelect : valueSelects) {
                if (existAttr(scan, valueSelect.getPredicate().getLeftAttribute())) {
                    orderedScans.add(scan);
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
        orderedScans.sort(Comparator.comparingInt(s -> s.getRelation().getTupleCount()));
        scans.sort(Comparator.comparingInt(s -> s.getRelation().getTupleCount()));
        orderedScans.addAll(scans);
        return orderedScans;
    }

    private List<Select> getJoinsByScan(List<Select> joins, Scan scan) {
        List<Select> target = new ArrayList<>();
        for (Select join : joins) {
            Attribute left = join.getPredicate().getLeftAttribute();
            Attribute right = join.getPredicate().getRightAttribute();
            if (existAttr(scan, left) || existAttr(scan, right)) {
                target.add(join);
            }
        }
        return target;
    }

    private List<Scan> getScansByJoin(List<Scan> scans, List<Select> joins, Scan first) {
        List<Scan> target = new ArrayList<>();
        for (Select join : joins) {
            Attribute left = join.getPredicate().getLeftAttribute();
            Attribute right = join.getPredicate().getRightAttribute();
            if (existAttr(first, left)) {
                target.add(findScanByAttr(scans, right));
            }
            if (existAttr(first, right)) {
                target.add(findScanByAttr(scans, left));
            }
        }
        return target;
    }

    private Scan findScanByAttr(List<Scan> scans, Attribute attribute) {
        for (Scan scan : scans) {
            if (scan.getRelation().getAttribute(attribute) != null) {
                return scan;
            }
        }
        return null;
    }

    private void addSelectByRight(List<Select> joins, List<Select> valueSelects, Operator operator) {
        Select select = (Select) operator;
        Predicate predicate = select.getPredicate();
        if (!predicate.equalsValue()) {
            joins.add(select);
        } else {
            valueSelects.add(select);
        }
    }
}
