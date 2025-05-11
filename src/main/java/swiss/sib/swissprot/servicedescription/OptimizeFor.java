package swiss.sib.swissprot.servicedescription;

public enum OptimizeFor {
	SPARQL(true), VIRTUOSO(false), QLEVER(true);

	private final boolean preferGroupBy;

	public boolean preferGroupBy() {
		return preferGroupBy;
	}

	OptimizeFor(boolean preferGroupBy) {
		this.preferGroupBy = preferGroupBy;
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
		}
		throw new IllegalStateException("Unexpected value: " + this);
	}

}
