package com.ddwanglife.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * 单词统计结果
 * @author Administrator
 *
 */
public class WordCountEntity implements Serializable {
	
	private static final long serialVersionUID = 3518776796426921776L;
	private long id;
	private String word;//单词内容
	private int count;//统计条数
	private Date createTime;//统计时间
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

}
