# poller

Golang library for data polling.

## About The Project

Poller - small library, that contains basic logic for data polling mechanisms.
One of the common library use cases is implementation
of [outbox pattern](https://medium.com/design-microservices-architecture-with-patterns/outbox-pattern-for-microservices-architectures-1b8648dfaa27).

Features:

- laconic api
- context package support
- concurrency limit

## Usage

```
process := poller.New(3*time.Second, 2, 2, 2, func(ctx context.Context, limit int) ([]poller.Task, bool) {
	return fetchTasksFromDatabase(limit)
})
process.Start()
```

## License

Poller is licensed under the Apache License, Version 2.0. See [LICENSE](./LICENCE.md)
for the full license text.

## Contact

- Email: `cherkashin.evgeny.viktorovich@gmail.com`
- Telegram: `@evgeny_cherkashin`