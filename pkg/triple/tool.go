package triple

import "github.com/dubbogo/triple/pkg/config"

// addDefaultOption fill default options to @opt
func addDefaultOption(opt *config.Option) *config.Option {
	if opt == nil {
		opt = &config.Option{}
	}
	opt.SetEmptyFieldDefaultConfig()
	return opt
}
