package tests;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.hp.hpl.jena.vocabulary.XSD;

import dataunity.datastructdef.MetadataInfo;

public class MetadataInfoTest {
	
	// -------------------
	// Integers
	// -------------------
	@Test
	public void shouldIdentifyInteger() {
		// ToDo: make these into tests
		assertTrue(MetadataInfo.isInteger("5"));
		assertTrue(MetadataInfo.isInteger("50005"));
	}
	
	@Test
	public void shouldIdentifyNegativeInteger() {
		// ToDo: make these into tests
		assertTrue(MetadataInfo.isInteger("-5"));
		assertTrue(MetadataInfo.isInteger("-50005"));
	}
	
	@Test
	public void shouldNotIdentifyDecimalAsInteger() {
		assertFalse(MetadataInfo.isInteger("500.005"));
	}
	
	@Test
	public void shouldNotIdentifyEmptyAsInteger() {
		assertFalse(MetadataInfo.isInteger(""));
	}
	
	@Test
	public void shouldNotIdentifyNullAsInteger() {
		assertFalse(MetadataInfo.isInteger(null));
	}
	
	@Test
	public void shouldNotIdentifyTextAsInteger() {
		assertFalse(MetadataInfo.isInteger("text"));
	}
	
	@Test
	public void shouldIgnoreWhitespaceAroundInteger() {
		assertTrue(MetadataInfo.isInteger(" 555"));
		assertTrue(MetadataInfo.isInteger("555 "));
		assertTrue(MetadataInfo.isInteger(" 555 "));
	}
	
	@Test
	public void shouldNotAllowWhitespaceInInteger() {
		assertFalse(MetadataInfo.isInteger("5 5"));
		assertFalse(MetadataInfo.isInteger("5 5 5"));
	}
	
	
	// -------------------
	// Decimals
	// -------------------
	@Test
	public void shouldIdentifyDecimal() {
		assertTrue(MetadataInfo.isDecimal("5.5"));
		assertTrue(MetadataInfo.isDecimal("50005.00005"));
		assertTrue(MetadataInfo.isDecimal("53.8673898723498723036"));
		assertTrue(MetadataInfo.isDecimal("-1.90700823402983423872442"));
	}
	
	@Test
	public void shouldIdentifyNegativeDecimal() {
		assertTrue(MetadataInfo.isDecimal("-5.5"));
		assertTrue(MetadataInfo.isDecimal("-50005.00005"));
	}
	
	@Test
	public void shouldNotIdentifyIntegerAsDecimal() {
		assertFalse(MetadataInfo.isDecimal("500"));
	}
	
	@Test
	public void shouldNotIdentifyEmptyAsDecimal() {
		assertFalse(MetadataInfo.isDecimal(""));
	}
	
	@Test
	public void shouldNotIdentifyNullAsDecimal() {
		assertFalse(MetadataInfo.isDecimal(null));
	}
	
	@Test
	public void shouldNotIdentifyTextAsDecimal() {
		assertFalse(MetadataInfo.isInteger("text"));
	}
	
	@Test
	public void shouldIgnoreWhitespaceAroundDecimal() {
		assertTrue(MetadataInfo.isDecimal(" 5.5"));
		assertTrue(MetadataInfo.isDecimal("5.5 "));
		assertTrue(MetadataInfo.isDecimal(" 5.5 "));
	}
	
	@Test
	public void shouldNotAllowWhitespaceInDecimal() {
		assertFalse(MetadataInfo.isDecimal("5 .5"));
		assertFalse(MetadataInfo.isDecimal("5.5 0"));
	}
	
	// -------------------
	// Integer type guessing
	// -------------------
	
	@Test
	public void shouldGuessIntegerType() {
		List<String> vals = new ArrayList<String>();
		vals.add("5");
		vals.add("555");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.integer.toString());
	}
	
	@Test
	public void shouldGuessIntegerTypeFromOneInstance() {
		List<String> vals = new ArrayList<String>();
		vals.add("5");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.integer.toString());
	}
	
	@Test
	public void shouldGuessIntegerTypeWithBlanks() {
		List<String> vals = new ArrayList<String>();
		vals.add("5");
		vals.add("");
		vals.add(null);
		vals.add("5");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.integer.toString());
	}
	
	// -------------------
	// Decimal type guessing
	// -------------------
	
	@Test
	public void shouldGuessDecimalType() {
		List<String> vals = new ArrayList<String>();
		vals.add("5.5");
		vals.add("555.555");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.decimal.toString());
	}
	
	@Test
	public void shouldGuessDecimalTypeFromOneInstance() {
		List<String> vals = new ArrayList<String>();
		vals.add("5.5");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.decimal.toString());
	}
	
	@Test
	public void shouldGuessDecimalTypeFromMixed() {
		List<String> vals = new ArrayList<String>();
		vals.add("5.5");
		vals.add("5");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.decimal.toString());
	}
	
	@Test
	public void shouldGuessDecimalTypeWithBlanks() {
		List<String> vals = new ArrayList<String>();
		vals.add("5.5");
		vals.add("");
		vals.add(null);
		vals.add("555.555");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.decimal.toString());
	}
	
	// -------------------
	// String type guessing
	// -------------------
	
	@Test
	public void shouldGuessStringType() {
		List<String> vals = new ArrayList<String>();
		vals.add("5.5");
		vals.add("TEXT");
		vals.add("555.555");
		assertEquals(MetadataInfo.guessXSDType(vals), XSD.xstring.toString());
	}
}
