package com.baixing.search.geli.Unittest

import org.scalatest.FlatSpec

/**
 * Created by abzyme-baixing on 14-11-24.
 */
class ExampleSpec extends FlatSpec{
	behavior of "An empty Set"

	it should "have size 0" in {
		assert(Set.empty.size === 0)
	}

	it should "produce NoSuchElementException when head is invoked" in {
		intercept[NoSuchElementException] {
			Set.empty.head
		}
	}
}

