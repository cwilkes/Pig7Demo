package org.seattlehadoop.ngram;

public class WebPage {

	private int m_userID;
	private String m_pageName;

	public WebPage(int userID, String pageName) {
		m_userID = userID;
		m_pageName = pageName;
	}

	public int getUserID() {
		return m_userID;
	}

	public String getPageName() {
		return m_pageName;
	}

}