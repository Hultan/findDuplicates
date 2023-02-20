package main

import (
	"testing"
)

func Test_directoryExists(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Empty", args{""}, false},
		{"Non-existing dir", args{"/home/per/code2"}, false},
		{"File", args{"/home/per/code/findDuplicates/go.mod"}, false},
		{"Dir", args{"/home/per/code/findDuplicates"}, true},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				if got := directoryExists(tt.args.filePath); got != tt.want {
					t.Errorf("directoryExists() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}
