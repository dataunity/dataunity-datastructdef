package tests;

import static org.junit.Assert.*;

import org.junit.Test;

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
	
	
	// -------------------
	// Decimals
	// -------------------
	@Test
	public void shouldIdentifyDecimal() {
		assertTrue(MetadataInfo.isDecimal("5.5"));
		assertTrue(MetadataInfo.isDecimal("50005.00005"));
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
}
