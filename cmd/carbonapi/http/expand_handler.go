package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ansel1/merry"
	"github.com/go-graphite/carbonapi/carbonapipb"
	"github.com/go-graphite/carbonapi/cmd/carbonapi/config"
	"github.com/go-graphite/carbonapi/date"
	utilctx "github.com/go-graphite/carbonapi/util/ctx"
	pbv3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/zapwriter"
	uuid "github.com/satori/go.uuid"
)

// Find handler and it's helper functions
func expandList(multiGlobs *pbv3.MultiGlobResponse) ([]byte, error) {
	var b bytes.Buffer
	var matches = make([]string, 0)

	for _, globs := range multiGlobs.Metrics {
		for _, g := range globs.Matches {
			if strings.HasPrefix(g.Path, "_tag") {
				continue
			}
      matches = append(matches, g.Path)
		}
	}

	err := json.NewEncoder(&b).Encode(struct {
    Results []string `json:"results"`
	}{
		Results: matches},
	)
	return b.Bytes(), err
}

func expandHandler(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	uid := uuid.NewV4()
	// TODO: Migrate to context.WithTimeout
	// ctx, _ := context.WithTimeout(context.TODO(), config.Config.ZipperTimeout)
	ctx := utilctx.SetUUID(r.Context(), uid.String())
	username, _, _ := r.BasicAuth()
	requestHeaders := utilctx.GetLogHeaders(ctx)

	format, ok, formatRaw := getFormat(r, jsonFormat)
	jsonp := r.FormValue("jsonp")

	qtz := r.FormValue("tz")
	from := r.FormValue("from")
	until := r.FormValue("until")
	from64 := date.DateParamToEpoch(from, qtz, timeNow().Add(-time.Hour).Unix(), config.Config.DefaultTimeZone)
	until64 := date.DateParamToEpoch(until, qtz, timeNow().Unix(), config.Config.DefaultTimeZone)

	query := r.Form["query"]
	srcIP, srcPort := splitRemoteAddr(r.RemoteAddr)

	accessLogger := zapwriter.Logger("access")
	var accessLogDetails = carbonapipb.AccessLogDetails{
		Handler:        "find",
		Username:       username,
		CarbonapiUUID:  uid.String(),
		URL:            r.URL.RequestURI(),
		PeerIP:         srcIP,
		PeerPort:       srcPort,
		Host:           r.Host,
		Referer:        r.Referer(),
		URI:            r.RequestURI,
		Format:         formatRaw,
		RequestHeaders: requestHeaders,
	}

	logAsError := false
	defer func() {
		deferredAccessLogging(accessLogger, &accessLogDetails, t0, logAsError)
	}()

	if !ok || format != jsonFormat {
		http.Error(w, "unsupported format: "+formatRaw, http.StatusBadRequest)
		accessLogDetails.HTTPCode = http.StatusBadRequest
		accessLogDetails.Reason = "unsupported format: " + formatRaw
		logAsError = true
		return
	}

	var pv3Request pbv3.MultiGlobRequest

	pv3Request.Metrics = query
  pv3Request.StartTime = from64
	pv3Request.StopTime = until64

	if len(pv3Request.Metrics) == 0 {
		http.Error(w, "missing parameter `query`", http.StatusBadRequest)
		accessLogDetails.HTTPCode = http.StatusBadRequest
		accessLogDetails.Reason = "missing parameter `query`"
		logAsError = true
		return
	}

	multiGlobs, stats, err := config.Config.ZipperInstance.Find(ctx, pv3Request)
	if stats != nil {
		accessLogDetails.ZipperRequests = stats.ZipperRequests
		accessLogDetails.TotalMetricsCount += stats.TotalMetricsCount
	}
	if err != nil {
		returnCode := merry.HTTPCode(err)
		if returnCode != http.StatusOK || multiGlobs == nil {
			// Allow override status code for 404-not-found replies.
			if returnCode == http.StatusNotFound {
				returnCode = config.Config.NotFoundStatusCode
			}

			if returnCode < 300 {
				multiGlobs = &pbv3.MultiGlobResponse{Metrics: []pbv3.GlobResponse{}}
			} else {
				http.Error(w, http.StatusText(returnCode), returnCode)
				accessLogDetails.HTTPCode = int32(returnCode)
				accessLogDetails.Reason = err.Error()
				// We don't want to log this as an error if it's something normal
				// Normal is everything that is >= 500. So if config.Config.NotFoundStatusCode is 500 - this will be
				// logged as error
				if returnCode >= 500 {
					logAsError = true
				}
				return
			}
		}
	}
	var b []byte
	var err2 error
	b, err2 = expandList(multiGlobs)
	err = merry.Wrap(err2)

	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		accessLogDetails.HTTPCode = http.StatusInternalServerError
		accessLogDetails.Reason = err.Error()
		logAsError = true
		return
	}

	writeResponse(w, http.StatusOK, b, format, jsonp)
}
