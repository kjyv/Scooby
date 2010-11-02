/*
 * in the final application this will be changed to a 2-element-integer-array for faster access / smaller size
 * 
*/
class MedlineTokenLocation
{
	int pmid;
	MedlineTokenParentTag parentTag;
	public MedlineTokenLocation(int pmid, MedlineTokenParentTag parentTag)
	{
		this.pmid = pmid;
		this.parentTag = parentTag;
	}
}