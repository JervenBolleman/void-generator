package swiss.sib.swissprot.servicedescription;

public enum OptimizeFor {
	SPARQL, VIRTUOSO, QLEVER;

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
		}
		throw new IllegalStateException("Unexpected value: " + this);
	}

}
