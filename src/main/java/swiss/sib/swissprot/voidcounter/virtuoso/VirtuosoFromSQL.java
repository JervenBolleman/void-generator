package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public class VirtuosoFromSQL {
	private static final Logger logger = LoggerFactory.getLogger(VirtuosoFromSQL.class);

	public static long countDistinctLongResultsFromVirtuoso(RepositoryConnection localConnection, String sql)
			throws QueryEvaluationException, RepositoryException {
		// We are using BitSets for much faster distinct counts.
		Map<Integer, DistinctIntegerCounter> intCounters = new HashMap<>();
		{
			VirtuosoRepositoryConnection virtuosoRepositoryConnection = (VirtuosoRepositoryConnection) localConnection;
			Connection vrc = virtuosoRepositoryConnection.getQuadStoreConnection();
			try (java.sql.Statement stat = vrc.createStatement()) {
				stat.setFetchSize(DistinctIntegerCounter.CACHE_SIZE);
				loopOverResultSetWithLongColumnToCountDistinct(sql, intCounters, stat);
			} catch (SQLException e) {
				logger.debug("FAILED connection:" + sql);
				throw new RepositoryException(e);
			}
		}
		return intCounters.values().stream().mapToLong(DistinctIntegerCounter::cardinality).sum();
	}

	private static void loopOverResultSetWithLongColumnToCountDistinct(String sql,
			Map<Integer, swiss.sib.swissprot.voidcounter.virtuoso.DistinctIntegerCounter> intCounters, java.sql.Statement stat)
			throws QueryEvaluationException {

		try (ResultSet res = stat.executeQuery(sql)) {
			res.setFetchDirection(ResultSet.FETCH_FORWARD);
			while (res.next()) {
				long iriId = res.getLong(1);

				// This is a bit of nasty logic.
				// Assumption the leftmost bit in the iriID is not set
				// i.e. it is positive and can be disregard

				// First find a 'key' to get a set
				// we shift by 31 bits taking to get a positive key.
				int setSelector = (int) (iriId >>> 31);
				// We then convert the right most bits into an integer.
				// This will always be positive.
				int rightMost = (int) (iriId & 0x7fffffff);
				// If we had shifted by 32 bits we would have had issues
				// with negative numbers that are not valid to be put into the bitsets.
				DistinctIntegerCounter bitSet = intCounters.get(setSelector);
				if (bitSet == null) {
					bitSet = new DistinctIntegerCounter();
					intCounters.put(setSelector, bitSet);
				}
				bitSet.add(rightMost);
			}
		} catch (SQLException e) {
			logger.debug("FAILED:" + e.getMessage());
			throw new QueryEvaluationException(e);
		}
	}

	public static long getSingleLongFromSql(String sql, VirtuosoRepositoryConnection connection) {
		Connection vrc = connection.getQuadStoreConnection();
		try (java.sql.Statement stat = vrc.createStatement()) {
			stat.setFetchSize(1);
			try (ResultSet rs = stat.executeQuery(sql)) {
				if (rs.next())
					return rs.getLong(1);
				else {
					logger.debug("FAILED to get result for:" + sql);
					throw new RepositoryException("FAILED to get result for:" + sql);
				}
			}
		} catch (SQLException e) {
			logger.debug("FAILED connection:" + sql);
			throw new RepositoryException(e);
		}
	}
}
