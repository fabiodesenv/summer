package br.edu.ufam.icomp.exception;

public class NoResultException extends RuntimeException {

	private static final long serialVersionUID = 1998L;

	public NoResultException() {
		super();
	}

	public NoResultException(String message) {
		super(message);
	}
}