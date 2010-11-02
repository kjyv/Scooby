import java.util.Vector;

class MedlineToken
{
	String token;
	Vector<MedlineTokenLocation> locations;
	
	public MedlineToken(String token, int pmid, Vector<MedlineTokenLocation> locations)
	{
		this.token = token;
		this.locations = locations;
	}
}