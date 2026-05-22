export class OnlyDogeError extends Error {
  public readonly statusCode: number;

  public constructor(message: string, statusCode = 400, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
    this.statusCode = statusCode;
  }
}

export class ValidationError extends OnlyDogeError {
  public constructor(message: string) {
    super(message, 400);
  }
}

export class ConflictError extends OnlyDogeError {
  public constructor(message: string) {
    super(message, 400);
  }
}

export class UnauthorizedError extends OnlyDogeError {
  public constructor(message = 'unauthorized') {
    super(message, 401);
  }
}

export class NotFoundError extends OnlyDogeError {
  public constructor(message = 'not found') {
    super(message, 404);
  }
}

export class TooEarlyError extends OnlyDogeError {
  public constructor(message: string) {
    super(message, 425);
  }
}

export class InfrastructureError extends OnlyDogeError {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, 500, options);
  }
}
