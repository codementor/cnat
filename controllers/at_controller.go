/*
Copyright 2020 kenny.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	//TODO (kensipe): need imports
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnatv1alpha1 "github.com/codementor/cnat/api/v1alpha1"
)

// AtReconciler reconciles a At object
type AtReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// todo: 1 rbac
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cnat.d2iq.com,resources=ats,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cnat.d2iq.com,resources=ats/status,verbs=get;update;patch

// Reconcile provides logic for Ats controller
func (r *AtReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {

	// todo:  2 logger with log message
	// name logger
	logger := r.Log.WithValues("namespace", req.NamespacedName, "at", req.Name)
	logger.Info("== Reconciling At")

	// todo: 3 fetch
	// Fetch the At instance
	instance := &cnatv1alpha1.At{}
	err := r.Get(context.TODO(), req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request - return and don't requeue:
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request:
		return reconcile.Result{}, err
	}

	// todo: 4 set status
	// If no phase set, default to pending (the initial phase):
	if instance.Status.Phase == "" {
		instance.Status.Phase = cnatv1alpha1.PhasePending
		r.Recorder.Event(instance, "Normal", "PhaseChange", cnatv1alpha1.PhasePending)
	}

	//TODO (kensipe): 6 controller logic
	/////  provide in labs

	// Now let's make the main case distinction: implementing
	// the state diagram PENDING -> RUNNING -> DONE
	switch instance.Status.Phase {
	case cnatv1alpha1.PhasePending:
		logger.Info("Phase: PENDING")
		// As long as we haven't executed the command yet, we need to check if it's time already to act:
		logger.Info("Checking schedule", "Target", instance.Spec.Schedule)
		// Check if it's already time to execute the command with a tolerance of 2 seconds:
		d, err := timeUntilSchedule(instance.Spec.Schedule)
		if err != nil {
			logger.Error(err, "Schedule parsing failure")
			// Error reading the schedule. Wait until it is fixed.
			return reconcile.Result{}, err
		}
		logger.Info("Schedule parsing done", "Result", fmt.Sprintf("diff=%v", d))
		if d > 0 {
			// Not yet time to execute the command, wait until the scheduled time
			return reconcile.Result{RequeueAfter: d}, nil
		}
		logger.Info("It's time!", "Ready to execute", instance.Spec.Command)
		instance.Status.Phase = cnatv1alpha1.PhaseRunning
		r.Recorder.Event(instance, "Normal", "PhaseChange", cnatv1alpha1.PhaseRunning)
	case cnatv1alpha1.PhaseRunning:
		logger.Info("Phase: RUNNING")
		pod := newPodForCR(instance)
		// Set At instance as the owner and controller
		if err := controllerutil.SetControllerReference(instance, pod, r.Scheme); err != nil {
			// requeue with error
			return reconcile.Result{}, err
		}
		found := &corev1.Pod{}
		err = r.Get(context.TODO(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, found)
		// Try to see if the pod already exists and if not
		// (which we expect) then create a one-shot pod as per spec:
		if err != nil && errors.IsNotFound(err) {
			err = r.Create(context.TODO(), pod)
			if err != nil {
				// requeue with error
				return reconcile.Result{}, err
			}
			logger.Info("Pod launched", "name", pod.Name)
		} else if err != nil {
			// requeue with error
			return reconcile.Result{}, err
		} else if found.Status.Phase == corev1.PodFailed || found.Status.Phase == corev1.PodSucceeded {
			logger.Info("Container terminated", "reason", found.Status.Reason, "message", found.Status.Message)
			instance.Status.Phase = cnatv1alpha1.PhaseDone
			r.Recorder.Event(instance, "Normal", "PhaseChange", cnatv1alpha1.PhaseDone)
		} else {
			// don't requeue because it will happen automatically when the pod status changes
			return reconcile.Result{}, nil
		}
	case cnatv1alpha1.PhaseDone:
		logger.Info("Phase: DONE")
		return reconcile.Result{}, nil
	default:
		logger.Info("NOP")
		return reconcile.Result{}, nil
	}

	////

	//TODO (kensipe): 5 update status
	// Update the At instance, setting the status to the respective phase:
	err = r.Status().Update(context.TODO(), instance)
	if err != nil {
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up controller with mgr
func (r *AtReconciler) SetupWithManager(mgr ctrl.Manager) error {

	return ctrl.NewControllerManagedBy(mgr).
		For(&cnatv1alpha1.At{}).
		Owns(&cnatv1alpha1.At{}).
		Owns(&corev1.Pod{}).
		Complete(r)

	//TODO (kensipe): need to watch pods
}

//TODO (kensipe): 7 support functions

// newPodForCR returns a busybox pod with the same name/namespace as the cr
func newPodForCR(cr *cnatv1alpha1.At) *corev1.Pod {
	labels := map[string]string{
		"app": cr.Name,
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-pod",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "busybox",
					Image:   "busybox",
					Command: strings.Split(cr.Spec.Command, " "),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
}

// timeUntilSchedule parses the schedule string and returns the time until the schedule.
// When it is overdue, the duration is negative.
func timeUntilSchedule(schedule string) (time.Duration, error) {
	now := time.Now().UTC()
	layout := "2006-01-02T15:04:05Z"
	s, err := time.Parse(layout, schedule)
	if err != nil {
		return time.Duration(0), err
	}
	return s.Sub(now), nil
}
