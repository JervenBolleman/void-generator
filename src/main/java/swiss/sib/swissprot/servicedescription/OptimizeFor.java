package swiss.sib.swissprot.servicedescription;

public enum OptimizeFor {
	SPARQL(true, false), VIRTUOSO(false, true), QLEVER(false, true), SPARQLUNION(true, true);

	private final boolean preferGroupBy;
	private final boolean allInUnionGraph;

	public boolean preferGroupBy() {
		return preferGroupBy;
	}
	
	/** 
	 * If the store supports an union graph as the default graph.
	 * @return
	 */
	public boolean allInUnionGraph() {
		return allInUnionGraph;
	}

	OptimizeFor(boolean preferGroupBy, boolean allInUnionGraph) {
		this.preferGroupBy = preferGroupBy;
		this.allInUnionGraph = allInUnionGraph;	
	}

	static OptimizeFor fromString(String optimizeFor) {
		for (OptimizeFor of : OptimizeFor.values()) {
			if (of.name().equalsIgnoreCase(optimizeFor)) {
				return of;
			}
		}
		return SPARQL;
	}

	public String dir() {
		switch (this) {
		case SPARQL:
			return "sparql";
		case VIRTUOSO:
			return "virtuoso";
		case QLEVER:
			return "qlever";
		case SPARQLUNION:
			return "sparqlunion";
		}
		throw new IllegalStateException("Unexpected value: " + this);
	}

	

}
