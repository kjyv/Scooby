import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.*;

class Assignment1
{
	final static String REGEXP_SPLIT_TOKENS = "\\s+";	// \s+ , multiple white spaces TODO: check if text contains markup tags
	static String indexFilePath = "inverted_index.dat";

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args)
	{
		System.out.println(args.length);
		if(true) return;
		
		if(args.length == 0)
		{
			System.out.println("usage:\nAssignment1 -index [xmlfile]\nor\nAssignment1 [token1] [token2] ...\nor\nAssignment1 \"[token1] [token2]...\"");
			return;
		}
		else if(args.length >= 2 && args[0].compareTo("-index") == 0)
		{
			String filename = args[1];
			try {
				HashMap<String, Vector<MedlineTokenLocation>> invertedIndex = buildIndex(filename);
				if(args.length >= 3)
				{
					if (args[2] == "hash")
					{
						indexFilePath = "hash_" + indexFilePath;
						buildHashIndex(invertedIndex);
					}
					else if(args[2] == "sql")
					{
						indexFilePath = "sql_" + indexFilePath;
						buildSQLIndex(invertedIndex);
					}
					// else if(args[2] == ...)
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
		{
			File indexFile = new File(indexFilePath);
			if(!indexFile.exists())
			{
				System.out.println("ERROR: must build index first, use Assignment1 -index [xmlfile]");
				return;
			}
			
			// phrase search if first char of first arg is a quote and last char of last arg is a quote
			if(args[0].charAt(0) == '"' && args[args.length-1].charAt(args[args.length-1].length()-1) == '"')
			{
				args[0] = args[0].substring(1);	// skip first char
				args[args.length-1] = args[args.length-1].substring(0, args.length); // leave out last char
				phraseQuery(args);
			}
			else
			{
				boolQuery(args);
			}
		}
		// TODO: get filename from arguments		
		
	}

	public static void boolQuery(String[] querytokens)
	{
		
	}
	
	public static void phraseQuery(String[] querytokens)
	{
		
	}

	public static HashMap<String, Vector<MedlineTokenLocation>> buildIndex(String filename) throws IOException
	{
		// read file into
		File f = new File(filename);
		char[] cbuf = new char[(int)f.length()];
		InputStreamReader in = new InputStreamReader(new FileInputStream(f), "UTF-8");
		in.read(cbuf);
		String xml = new String(cbuf);
		// don't confuse <MedlineCitationSet> with <MedlineCitation owner=""...>
		Pattern pCitation = Pattern.compile("<MedlineCitation( .+?)?>(.+?)</MedlineCitation>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pAbstractTitle = Pattern.compile("<AbstractTitle.*?>(.+?)</AbstractTitle>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pAbstractText = Pattern.compile("<AbstractText.*?>(.+?)</AbstractText>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pPmid = Pattern.compile("<pmid>(\\d+)</pmid>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Matcher mCitation = pCitation.matcher(xml);
		
		String medlineCitation;
		Matcher mAbstractTitle, mAbstractText, mPmid;
		// naive tokenization: 99084 different tokens
		HashMap<String, Vector<MedlineTokenLocation>> invertedIndex = new HashMap<String, Vector<MedlineTokenLocation>>(100000, 1.0f);
		
		while(mCitation.find())
		{
			medlineCitation = mCitation.group(2);
			mPmid = pPmid.matcher(medlineCitation);
			if(mPmid.find() == false)
				continue;
			int pmid = Integer.parseInt(mPmid.group(1));
			
			// search for tokens in <AbstractTitle>
			mAbstractTitle = pAbstractTitle.matcher(medlineCitation);
			while(mAbstractTitle.find())
			{
				extractTokens(
							mAbstractTitle.group(1).toLowerCase(),
							MedlineTokenParentTag.ABSTRACT_TITLE,
							pmid,
							invertedIndex
				);
			}
			
			// search for tokens in <AbstractText>
			mAbstractText = pAbstractText.matcher(medlineCitation);
			while(mAbstractText.find())
			{
				extractTokens(
							mAbstractText.group(1).toLowerCase(),
							MedlineTokenParentTag.ABSTRACT_TEXT,
							pmid,
							invertedIndex
				);
			}
		}
		//System.out.println(invertedIndex.size());
		return invertedIndex;
	}
	
	// splits text to tokens and organizes them in a hash map
	public static void extractTokens(String text, MedlineTokenParentTag parentTag, int pmid, HashMap<String, Vector<MedlineTokenLocation>> invertedIndex)
	{
		String[] tokens = text.split(REGEXP_SPLIT_TOKENS);
		int len = tokens.length;
		String token;
		Vector<MedlineTokenLocation> locations;
		for(int i = 0; i < len; i++)
		{
			token = tokens[i];
			if((locations = invertedIndex.get(token)) != null)
			{
				// existing token, add location
				locations.add(new MedlineTokenLocation(pmid, parentTag));
			}
			else
			{
				// new token - create first location
				locations = new Vector<MedlineTokenLocation>();
				locations.add(new MedlineTokenLocation(pmid, parentTag));
				invertedIndex.put(token, locations);
			}
		}
	}
	
	// compresses the HashMap and saves it to a file
	public static void buildHashIndex(HashMap<String, Vector<MedlineTokenLocation>> invertedIndex) throws IOException
	{
		HashMap<String, int[][]> invertedCompressedIndex = new HashMap<String, int[][]>(100000, 1.0f);
		Set<String> keys = invertedIndex.keySet();
		for(String key : keys)
		{
			Vector<MedlineTokenLocation> locations = invertedIndex.get(key);
			int[][] compressedLocations = new int[locations.size()][2];
			for (int i = 0; i < locations.size(); i++)
			{
				MedlineTokenLocation mtl = locations.get(i);
				compressedLocations[i][0] = mtl.pmid;
				compressedLocations[i][1] = mtl.parentTag.ordinal();
			}
			invertedCompressedIndex.put(key, compressedLocations);
		}
		
		
		FileOutputStream fos = new FileOutputStream(indexFilePath);
		ObjectOutputStream out = new ObjectOutputStream(fos);
		out.writeObject(invertedCompressedIndex);
	}
	
	// stores contents of hashMap in a SQLite DB
	public static void buildSQLIndex(HashMap<String, Vector<MedlineTokenLocation>> invertedIndex)
	{
		
		/*
		File dbFile = new File("sqlite_db.dat");
		SqlJetDb db;
		try {
			db = SqlJetDb.open(dbFile, true);
			db.getOptions().setAutovacuum(true);
			// seit wann kann java inline klassendefinitionen...?
			db.runTransaction(new ISqlJetTransaction() {
			    public Object run(SqlJetDb db) throws SqlJetException {
					// has to be set for each transaction
					db.getOptions().setUserVersion(1);
					return true;
			    }
	        }, SqlJetTransactionMode.WRITE);
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			try {            
				String createTableQuery = "CREATE TABLE token_doc (token VARCHAR(128), doc_id INTEGER)";
				String createIndexQuery = "CREATE INDEX the_index ON token_doc (token, doc_id)";
				db.createTable(createTableQuery);
				db.createIndex(createIndexQuery);
				
			} finally {
				db.commit();
			}
	        db.close();
		} catch (SqlJetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
	}
}

/*
FileInputStream fis = null;
ObjectInputStream in = null
*/


















