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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.*;

class Assignment1
{
	final static String REGEXP_SPLIT_TOKENS = "\\s+";	// \s+ , multiple white spaces TODO: check if text contains markup tags
    final static String INDEX_TABLE_NAME = "index_doc";
    final static String TOKEN_INDEX = "token_index";
    final static String DOCUMENTS_TABLE_NAME = "documents";
	final static String indexFileDBPath = "index.db";
	static String indexFilePath = "inverted_index.dat";
		
	@SuppressWarnings("deprecation")
	public static void main(String[] args)
	{

		if(args.length == 0)
		{
			System.out.println("usage:\nAssignment1 -index [xmlfile]\nor\nAssignment1 [token1] [token2] ...\nor\nAssignment1 \"[token1] [token2]...\"");
			return;
		}
		else if(args.length >= 2 && args[0].compareTo("-index") == 0)
		{
			String filename = args[1];
			try {
				
				HashMap<String, Vector<MedlineTokenLocation>> invertedIndex  = buildIndex(filename);
				  
				if(args.length >= 3)
				{
					if (args[2].compareTo("hash") == 0)
					{
						indexFilePath = "hash_" + indexFilePath;
						buildHashIndex(invertedIndex);
					}
					// else if(args[2] == ...)
				}
				else
				{
					buildSQLIndex(invertedIndex, filename);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
		{
			File indexFile = new File(indexFilePath);
			File indexFileDB = new File(indexFileDBPath);
			if(!indexFile.exists() && !indexFileDB.exists())
			{
				System.out.println("ERROR: must build index first, use Assignment1 -index [xmlfile]");
				return;
			}
			
			// phrase search if first char of first arg is a quote and last char of last arg is a quote
			if(args[0].charAt(0) == '"' && args[args.length-1].charAt(args[args.length-1].length()-1) == '"')
			{
				args[0] = args[0].substring(1);	// skip first char
				args[args.length-1] = args[args.length-1].substring(0, args.length); // leave out last char
				phraseQuerySQL(args);
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
		boolQuerySQL(querytokens);
	}
	
	public static Vector<Integer> boolQuerySQL(String[] querytokens)
	{
		File dbFile = new File(indexFileDBPath);
		SqlJetDb db;
        Vector<Integer> documents = new Vector<Integer>();

		try {
			db = SqlJetDb.open(dbFile, true);
			
			for (String token: querytokens)
			{
				//TODO: create array of document ids per token plus count, then sort by
				//count and then intersect starting with smallest set 
		        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
				try {				
		            ISqlJetTable table = db.getTable(INDEX_TABLE_NAME);
		            ISqlJetCursor cursor = table.lookup(TOKEN_INDEX, token);
		            
		            //documents for this token
		            Vector<Integer> current_docs = new Vector<Integer>();            
		            
		            try {
		                if (!cursor.eof()) {
		                    do {
		                    	current_docs.add((int)cursor.getInteger("doc_id"));
		                    } while(cursor.next());
		                }
		            } finally {
		                cursor.close();
		            }
		            
		            if (documents.size() > 0)
		            {
		        		//intersect with documents from before (AND)
		            	HashSet<Integer> current_docs_set = new HashSet<Integer>(current_docs);
		            	documents.retainAll(current_docs_set);
		            } else {
		            	//keep current documents for initial set
		            	documents = current_docs;
		            }
		            
				} finally { db.commit();}		
			}
		    db.close();
		    
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
		

		System.out.print("Found "+ documents.size()+ " documents matching your query");
		if (documents.size() == 0) { System.out.println(".");} else { System.out.println(":");}
		
		for (Integer doc : documents)
		{
			System.out.println(doc);
		}
		
		return documents;
	}
	
	public static void phraseQuerySQL(String[] querytokens)
	{
		//we have a phrase in arg[0] without ""
		
	}

	public static String readCorpus(String filename) throws IOException
	{
		File f = new File(filename);
		char[] cbuf = new char[(int)f.length()];
		InputStreamReader in = new InputStreamReader(new FileInputStream(f), "UTF-8");
		in.read(cbuf);
		return (new String(cbuf));
	}
	
	public static HashMap<String, Vector<MedlineTokenLocation>> buildIndex(String filename) throws IOException
	{
		String xml = readCorpus(filename);
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
				String title = mAbstractTitle.group(1).toLowerCase();
				
				extractTokens(
							title,
							MedlineTokenParentTag.ABSTRACT_TITLE,
							pmid,
							invertedIndex
				);	
			}
			
			// search for tokens in <AbstractText>
			mAbstractText = pAbstractText.matcher(medlineCitation);
			while(mAbstractText.find())
			{	
				String body = mAbstractText.group(1).toLowerCase();
				extractTokens(
							body,
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
	
	//convert vectors into arrays for file saving
	public static HashMap<String, int[][]> compressIndex(HashMap<String, Vector<MedlineTokenLocation>> invertedIndex)
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
		return invertedCompressedIndex;
		
	}
	
	public static void storeDocumentsSQL(String filename) throws IOException
	{
		String xml = readCorpus(filename);
		// don't confuse <MedlineCitationSet> with <MedlineCitation owner=""...>
		Pattern pCitation = Pattern.compile("<MedlineCitation( .+?)?>(.+?)</MedlineCitation>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pAbstractTitle = Pattern.compile("<AbstractTitle.*?>(.+?)</AbstractTitle>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pAbstractText = Pattern.compile("<AbstractText.*?>(.+?)</AbstractText>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Pattern pPmid = Pattern.compile("<pmid>(\\d+)</pmid>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.UNICODE_CASE);
		Matcher mCitation = pCitation.matcher(xml);
		
		String medlineCitation;
		Matcher mAbstractTitle, mAbstractText, mPmid;
		
		Vector<MedlineDocument> fullDocuments = new Vector<MedlineDocument>();
				
		File dbFile = new File(indexFileDBPath);
		SqlJetDb db;
		try {
			db = SqlJetDb.open(dbFile, true);
			
			while(mCitation.find())
			{
				medlineCitation = mCitation.group(2);
				mPmid = pPmid.matcher(medlineCitation);
				if(mPmid.find() == false)
					continue;
				int pmid = Integer.parseInt(mPmid.group(1));
				
				MedlineDocument document = null;
				
				mAbstractTitle = pAbstractTitle.matcher(medlineCitation);
				while(mAbstractTitle.find())
				{
					String title = mAbstractTitle.group(1).toLowerCase();
		
					document = new MedlineDocument(pmid, title, "");
					fullDocuments.add(document);
				}
				
				mAbstractText = pAbstractText.matcher(medlineCitation);
				while(mAbstractText.find())
				{	
					String body = mAbstractText.group(1).toLowerCase();
					
					if (document == null) { document = new MedlineDocument(pmid, "", body); fullDocuments.add(document);}
					else { 	document.body = body; }			
				}
			}

			db.beginTransaction(SqlJetTransactionMode.WRITE);        
			try {            
				ISqlJetTable table = db.getTable(DOCUMENTS_TABLE_NAME);

				for(MedlineDocument document: fullDocuments)
				//if(document != null)
				{
					//insert document into db
		            	    	
					table.insert(document.pmid, document.title, document.body);
					
				}
			} finally { db.commit();}
			db.close();
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	
	// saves index to a file
	public static void buildHashIndex(HashMap<String, Vector<MedlineTokenLocation>> invertedIndex) throws IOException
	{
		HashMap<String, int[][]> invertedCompressedIndex = compressIndex(invertedIndex);
		
		FileOutputStream fos = new FileOutputStream(indexFilePath);
		ObjectOutputStream out = new ObjectOutputStream(fos);
		out.writeObject(invertedCompressedIndex);
	}
	
	// stores contents of hashMap into an SQLite DB
	@SuppressWarnings("deprecation")
	public static void buildSQLIndex(HashMap<String, Vector<MedlineTokenLocation>> invertedIndex, String filename) throws IOException
	{
		File dbFile = new File(indexFileDBPath);
        dbFile.delete();

		SqlJetDb db;
		try {
			db = SqlJetDb.open(dbFile, true);
			db.getOptions().setAutovacuum(true);
			
			db.runTransaction(new ISqlJetTransaction() {
			    public Object run(SqlJetDb db) throws SqlJetException {
					db.getOptions().setUserVersion(1);
					return true;
			    }
	        }, SqlJetTransactionMode.WRITE);
			
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			try {            						
				db.createTable("CREATE TABLE " + INDEX_TABLE_NAME + " (token VARCHAR(128), doc_id INTEGER)");				
				db.createTable("CREATE TABLE " + DOCUMENTS_TABLE_NAME + " (pmid INTEGER, doc_title TEXT, doc_body TEXT)");
				db.createIndex("CREATE INDEX documents_index ON "+ DOCUMENTS_TABLE_NAME +" (pmid)");
			} finally {
				db.commit();
			}
						        
	        //insert token index into db
	        db.beginTransaction(SqlJetTransactionMode.WRITE);        
			try {            
	            ISqlJetTable table = db.getTable(INDEX_TABLE_NAME);
	            Set<String> keys = invertedIndex.keySet();
	    		for(String key : keys)
	    		{
	    			//put locations in set so we only insert unique pairs
	    			Vector<MedlineTokenLocation> locations = invertedIndex.get(key);
	    			HashSet<Integer> locationSet = new HashSet<Integer>(locations.size());
	    			for ( Iterator<MedlineTokenLocation> i = locations.iterator(); i.hasNext();)
	    			{    				
	    				int location = i.next().pmid;
	    				if(locationSet.add(new Integer(location))) { table.insert(key, location); }
	    			}
	    		}
	    		
	    		//create after inserts for speed up 
				db.createIndex("CREATE INDEX " + TOKEN_INDEX + " ON "+ INDEX_TABLE_NAME +" (token)");
			} finally { db.commit();}		

	        db.close();
	        
		} catch (SqlJetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		storeDocumentsSQL(filename);
		
	}
}

/*
FileInputStream fis = null;
ObjectInputStream in = null
*/


















