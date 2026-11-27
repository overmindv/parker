package app

import "github.com/overmindv/parker/internal/kafka"

// NewProducer создаёт producer из broker-ов Options приложения и регистрирует его очистку.
func (a *App) NewProducer() (*kafka.Producer, error) {
	p, err := kafka.NewProducerWithRuntime(a.tracing, a.opts.KafkaBrokers, a.kafkaSecurity())
	if err != nil {
		return nil, err
	}
	a.trackCloser(p.Close)

	return p, nil
}

// NewSubscriber создаёт consumer-group подписчика из broker-ов Options и регистрирует очистку.
func (a *App) NewSubscriber(group string, topics []string, handler kafka.Handler) (*kafka.Subscriber, error) {
	s, err := kafka.NewSubscriberWithRuntime(
		a.tracing, a.opts.KafkaBrokers, group, topics, handler, a.log,
		a.kafkaSecurity(),
	)
	if err != nil {
		return nil, err
	}
	a.trackCloser(s.Close)

	return s, nil
}

func (a *App) kafkaSecurity() kafka.Security {
	security := a.opts.KafkaSecurity
	security.Environment = a.opts.Environment

	return security
}
