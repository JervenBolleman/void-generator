package swiss.sib.swissprot.servicedescription;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import picocli.CommandLine.IVersionProvider;

public class GitPropertiesVersionFetch implements IVersionProvider {
	public String[] getVersion() throws Exception {
		Properties prop = new Properties();
		try (var is = this.getClass().getResourceAsStream("/void-generator-git.properties")) {
			if (is == null) {
				return new String[] { "unkown" };
			} else {
				prop.load(is);
			}
		}
		String commitId = prop.getProperty("git.commit.id.abbrev", "unknown");
		String dirty = prop.getProperty("git.dirty", "unknown");
		List<String> version = new ArrayList<>();
		String tag = prop.getProperty("git.tag", "");
		if (tag.isBlank()) {
			version.add(clean(commitId));
		} else {
			version.add(clean(tag));
		}
		if (dirty.equals("true")) {
			version.add("with-changes");
		}
		return version.toArray(new String[0]);
	}

	private String clean(String commitId) {
		return commitId.trim();
	}

}
