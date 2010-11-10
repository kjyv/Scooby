import java.util.Comparator;
import java.util.Vector;

/*
 * compares two Vector<Integer> s of the following form: {array_index, tokenHash, pmids.length} by pmids.length
 * so that PmidListIntersectionOrderComparator can be applied to a Vector<Vector<Integer>>
 * to get the search tokens in descending order of their pmidListLengths
 */
class PmidListIntersectionOrderComparator<T extends Vector<Integer>> implements Comparator<T>
{
	@Override
	public int compare(T o1, T o2) {
		return o1.get(1) - o2.get(1);
	}
}