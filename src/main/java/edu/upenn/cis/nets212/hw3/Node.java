package edu.upenn.cis.nets212.hw3;

public class Node {
	private boolean isArticle;
	private boolean isUser;
	private String id;
	private int categoryCount;
	private int userCount;
	private int articleCount;
	
	public Node(String id, boolean isArticle, boolean isUser) {
		this.id = id;
		this.isArticle = isArticle;
		this.isUser = isUser;
		
	}
	
	public String getId() {
		return this.id;
	}
	
	public boolean isArticle() {
		return this.isArticle;
	}
	
	public boolean isUser() {
		return this.isUser;
	}

}
