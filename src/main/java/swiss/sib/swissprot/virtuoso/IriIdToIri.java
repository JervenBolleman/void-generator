package swiss.sib.swissprot.virtuoso;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.LoggerFactory;

public class IriIdToIri {
	
	private IriIdToIri() {

	}

	public static String idToIri(final Connection quadStoreConnection, long id) {
		String selectDistinctGraphsIriVirtSql = "SELECT id_to_iri(iri_id_from_num(?))";

		try (PreparedStatement createStatement = quadStoreConnection.prepareStatement(selectDistinctGraphsIriVirtSql)) {
			createStatement.setLong(1, id);
			try (ResultSet rs = createStatement.executeQuery()) {
				while (rs.next()) {
					return rs.getString(1);
				}
			}
		} catch (SQLException e) {
			LoggerFactory.getLogger(IriIdToIri.class).debug("Error converting an id to an iri", e);
		}
		LoggerFactory.getLogger(IriIdToIri.class).debug("An unkown id was asked to be converted to an id"+id);
		return null;
	}
}
